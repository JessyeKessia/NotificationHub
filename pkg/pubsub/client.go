package pubsub

import( 
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"github.com/gorilla/websocket"
 )

 type Handler func(Message)

 type Message struct {
	Topic string `json:"topic"`
	Payload interface{} `json:"payload"`
 }

 type Envelope struct {
	Type string `json:"type"`
	RequestID string `json:"request_id,omitempty"`
	Topic string `json:"topic,omitempty"`
	Payload interface{} `json:"payload,omitempty"`
	Error     string      `json:"error,omitempty"`
}

type brokerConnection struct {
	url  string
	conn *websocket.Conn
	mu   sync.Mutex
}

type Client struct {
	brokers []string
	ring    *HashRing

	connections map[string]*brokerConnection
	handlers    map[string][]Handler

	mu         sync.RWMutex
	requestSeq uint64
}

func NewClient(brokers []string) (*Client, error) {
	if len(brokers) == 0 {
		return nil, errors.New("é necessário informar pelo menos um broker")
	}

	ring := NewHashRing()

	for _, brokerURL := range brokers {
		ring.Add(brokerURL)
	}

	client := &Client{
		brokers:     brokers,
		ring:        ring,
		connections: make(map[string]*brokerConnection),
		handlers:    make(map[string][]Handler),
	}

	log.Println("[PUBSUB-CLIENT] cliente criado com brokers:", brokers)

	return client, nil
}

// Connect mantém compatibilidade com a versão antiga da biblioteca.
// Ela cria um cliente com apenas um broker.
func Connect(url string) *Client {
	client, err := NewClient([]string{url})
	if err != nil {
		panic(err)
	}
	return client
}

func (c *Client) Publish(topic string, payload interface{})  error {
	if topic == "" {
		return errors.New("topic não pode ser vazio")
	}

	brokerURL, err := c.resolveBroker(topic)
	if err != nil {
		return err
	}

	conn, err := c.getConnection(brokerURL)
	if err != nil {
		return err
	}

	env := Envelope{
		Type:      "publish",
		Topic:     topic,
		Payload:   payload,
		RequestID: c.nextRequestID(),
	}
	
	log.Printf("[HASH] Publish topic='%s' -> broker='%s'", topic, brokerURL)

	return conn.writeJSON(env)
}

func (c *Client) Subscribe(topic string, handler Handler) error {
	if topic == "" {
		return errors.New("topic não pode ser vazio")
	}

	if handler == nil {
		return errors.New("handler não pode ser nil")
	}

	brokerURL, err := c.resolveBroker(topic)
	if err != nil {
		return err
	}

	conn, err := c.getConnection(brokerURL)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.handlers[topic] = append(c.handlers[topic], handler)
	c.mu.Unlock()

	env := Envelope{
		Type:      "subscribe",
		Topic:     topic,
		RequestID: c.nextRequestID(),
	}

	log.Printf("[HASH] Subscribe topic='%s' -> broker='%s'", topic, brokerURL)

	return conn.writeJSON(env)
}

func (c *Client) Unsubscribe(topic string) error {
	if topic == "" {
		return errors.New("topic não pode ser vazio")
	}

	brokerURL, err := c.resolveBroker(topic)
	if err != nil {
		return err
	}

	conn, err := c.getConnection(brokerURL)
	if err != nil {
		return err
	}

	c.mu.Lock()
	delete(c.handlers, topic)
	c.mu.Unlock()

	env := Envelope{
		Type:      "unsubscribe",
		Topic:     topic,
		RequestID: c.nextRequestID(),
	}

	log.Printf("[HASH] Unsubscribe topic='%s' -> broker='%s'", topic, brokerURL)

	return conn.writeJSON(env)
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for brokerURL, conn := range c.connections {
		_ = conn.conn.Close()
		delete(c.connections, brokerURL)

		log.Printf("[PUBSUB-CLIENT] conexão fechada com broker='%s'", brokerURL)
	}
}

func (c *Client) resolveBroker(topic string) (string, error) {
	brokerURL, err := c.ring.Get(topic)
	if err != nil {
		return "", err
	}

	return brokerURL, nil
}

func (c *Client) getConnection(brokerURL string) (*brokerConnection, error) {
	c.mu.RLock()
	existingConn, exists := c.connections[brokerURL]
	c.mu.RUnlock()

	if exists {
		return existingConn, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if existingConn, exists := c.connections[brokerURL]; exists {
		return existingConn, nil
	}

	conn, _, err := websocket.DefaultDialer.Dial(brokerURL, nil)
	if err != nil {
		return nil, fmt.Errorf("erro ao conectar no broker %s: %w", brokerURL, err)
	}

	brokerConn := &brokerConnection{
		url:  brokerURL,
		conn: conn,
	}

	c.connections[brokerURL] = brokerConn

	log.Printf("[PUBSUB-CLIENT] conectado ao broker='%s'", brokerURL)

	go c.readLoop(brokerConn)

	return brokerConn, nil
}

func (c *Client) readLoop(conn *brokerConnection) {
	for {
		_, data, err := conn.conn.ReadMessage()
		if err != nil {
			log.Printf("[PUBSUB-CLIENT] conexão perdida com broker='%s': %v", conn.url, err)
			return
		}

		var env Envelope

		if err := json.Unmarshal(data, &env); err != nil {
			log.Printf("[PUBSUB-CLIENT] mensagem inválida recebida do broker='%s': %s", conn.url, string(data))
			continue
		}

		switch env.Type {
		case "message":
			c.dispatch(env)
		case "ack":
			log.Printf("[ACK] broker='%s' request_id='%s' payload='%v'", conn.url, env.RequestID, env.Payload)

		case "error":
			log.Printf("[ERROR] broker='%s' request_id='%s' error='%s'", conn.url, env.RequestID, env.Error)

		default:
			log.Printf("[PUBSUB-CLIENT] mensagem recebida do broker='%s': %+v", conn.url, env)
		}
	}
}

func (c *Client) dispatch(env Envelope) {
	c.mu.RLock()
	handlers := append([]Handler(nil), c.handlers[env.Topic]...)
	c.mu.RUnlock()

	if len(handlers) == 0 {
		log.Printf("[PUBSUB-CLIENT] mensagem recebida para tópico sem handler local: %s", env.Topic)
		return
	}

	msg := Message{
		Topic:   env.Topic,
		Payload: env.Payload,
	}

	for _, handler := range handlers {
		go handler(msg)
	}
}

func (c *Client) nextRequestID() string {
	id := atomic.AddUint64(&c.requestSeq, 1)
	return fmt.Sprintf("req-%d", id)
}

func (bc *brokerConnection) writeJSON(v interface{}) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if err := bc.conn.WriteJSON(v); err != nil {
		return fmt.Errorf("erro ao enviar mensagem para broker %s: %w", bc.url, err)
	}

	return nil
}