package broker

import (
	"github.com/gorilla/websocket"
	"time"
	"log"
	"notificationhub/internal/protocol"
	"encoding/json"
)

// representa um cliente conectado ao broker
type Client struct {

	// id identificador único do cliente conectado
	ID string

	// conexao WebSocket do cliente
	Conn *websocket.Conn

	// fila assíncrona de saída
	Send chan []byte

	// sinalizador de fechamento do cliente
	LastPong time.Time
}

func (c *Client) readPump(b *Broker) {

	defer func () {
		b.Unregister(c)
		c.Conn.Close()
		
	}()

	c.Conn.SetPongHandler(func(string) error {

		c.LastPong = time.Now()

		return nil
	})

	for {

		_, message, err := c.Conn.ReadMessage()

		if err != nil {
			log.Println("read error:", err)
			break
		}

		var env protocol.Envelop

		err = json.Unmarshal(message, &env)

		if err != nil {

			c.SendError(
				"invalid_json",
				env.RequestID,
			)

			continue
		}

		switch env.Type {

		case "subscribe":
			if env.Topic == "" {
				c.SendError("topic_required", env.RequestID)
				continue
			}

			b.Subscribe(env.Topic, c)

			c.SendAck(
				"subscribed",
				env.RequestID,
			)
		case "unsubscribe":
			if env.Topic == "" {
				c.SendError("topic_required", env.RequestID)
				continue
			}

			b.Unsubscribe(env.Topic, c)

			c.SendAck(
				"unsubscribed",
				env.RequestID,
			)

		case "publish":

			if env.Topic == "" {
				c.SendError("topic_required", env.RequestID)
				continue
			}

			if env.Payload == nil {
				c.SendError("payload_required", env.RequestID)
				continue
			}

			status := b.Publish(
				env.Topic,
				env.Payload,
				"",
			)

			switch status {

			case PublishStatusPublished:
				c.SendAck(
					"published",
					env.RequestID,
				)
			case PublishStatusQueueFull:
				c.SendError("topic_queue_full", env.RequestID)
			case PublishStatusDiscardedNoSubscribers:
				c.SendError("discarded_no_subscribers", env.RequestID)
			default: 
				c.SendError("publish_failed", env.RequestID)
			}
		case "ping":
			c.SendAck(
				"pong",env.RequestID,
			)
		default:
			c.SendError("invalid_message_type", env.RequestID)
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(30 * time.Second)

	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {

		case msg, ok := <-c.Send:

			if !ok {
				log.Println("[CLIENT] canal de envio fechado:", c.ID)
				return
			}

			if err := c.Conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Println("[CLIENT] erro ao escrever mensagem:", err)
				return
			}

		case <-ticker.C:

			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Println("[CLIENT] erro ao enviar ping:", err)
				return
			}
		}
	}
}

func (c *Client) SendAck(
	message string,
	requestID string,
) {

	response := protocol.Envelop{
		Type:      "ack",
		Payload:   message,
		RequestID: requestID,
	}

	data, _ := json.Marshal(response)

	c.Send <- data
}

func (c *Client) SendError(
	errMsg string,
	requestID string,
) {

	response := protocol.Envelop{
		Type:      "error",
		Error:     errMsg,
		RequestID: requestID,
	}

	data, _ := json.Marshal(response)

	c.Send <- data
}

func (b * Broker) Unregister(client *Client) {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	delete(b.Clients, client.ID)

	for topicName, topic := range b.Topics {
		if _, exists := topic.Subscribers[client]; exists {
			delete(topic.Subscribers, client)

			log.Printf("[CLIENT] cliente %s removido do tópico '%s'", client.ID, topicName)

			if len(topic.Subscribers) == 0 {
				delete(b.Topics, topicName)
				close(topic.Queue)

				log.Printf("[TOPIC] tópico removido por desconexão do último subscriber: %s", topicName)
			}
		}
	}

	close(client.Send)

	log.Printf("[CLIENT] cliente desconectado e removido: %s", client.ID)
}