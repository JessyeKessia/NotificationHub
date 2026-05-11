package broker

import (
	"notificationhub/internal/protocol"
	"log"
	"sync"
	"encoding/json"
)

type Broker struct {
	Topics  map[string]*Topic
	Clients map[string]*Client
	Peers   map[string]*Peer
	Mutex   sync.RWMutex
}

func NewBroker() *Broker {
	return &Broker{
		Topics:  make(map[string]*Topic),
		Clients: make(map[string]*Client),
		Peers:   make(map[string]*Peer),
	}
}

func (b *Broker) Publish(
	topic string,
	payload interface{},
	// origin é o endereço do broker que originou a mensagem, 
	// usado para evitar loops de replicação
	origin string,
) {

	b.Mutex.RLock()

	t, exists := b.Topics[topic]

	b.Mutex.RUnlock()

	if !exists {

		log.Println("Tópico não existe: mensagem descartada do", topic)

		return
	}

	log.Printf("Publicando mensagem no tópico: %s para %d subscribers", topic, len(t.Subscribers))
	
	envelop := protocol.Envelop{
		Type:    "message",
		Topic:   topic,
		Payload: payload,
	}

	// envia para a fila do tópico
	select {
	case t.Queue <- envelop:
		log.Printf("Mensagem enfileirada com sucesso para o tópico: %s", topic)
	default:
		log.Println("Fila cheia: mensagem descartada do", topic)
	}

	// replica para outros brokers
	data, _ := json.Marshal(envelop)
	b.Forward(topic, data, origin)
}
func (b *Broker) Subscribe(
	topic string,
	client *Client,
) {

	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	t, exists := b.Topics[topic]

	if !exists {

		t = NewTopic(topic, b)

		b.Topics[topic] = t
	}

	t.Subscribers[client] = true

	log.Println("cliente inscrito no tópico:", topic)
}

func (b *Broker) Unsubscribe(
	topic string,
	client *Client,
) {

	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	t, exists := b.Topics[topic]

	if !exists {
		log.Println("Tópico não existe: mensagem ignorada")
		return
	}

	delete(t.Subscribers, client)

	t.checkEmpty()

	log.Println("cliente desinscrito do tópico:", topic)
}