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

	defer c.Conn.Close()

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

			b.Subscribe(env.Topic, c)

			c.SendAck(
				"subscribed",
				env.RequestID,
			)

		case "unsubscribe":

			b.Unsubscribe(env.Topic, c)

			c.SendAck(
				"unsubscribed",
				env.RequestID,
			)

		case "publish":

			b.Publish(
				env.Topic,
				env.Payload,
				"",
			)

			c.SendAck(
				"published",
				env.RequestID,
			)
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(30 * time.Second)

	for {
		select {
		case msg := <-c.Send:
			c.Conn.WriteMessage(websocket.TextMessage, msg)
		case <-ticker.C:
			c.Conn.WriteMessage(websocket.PingMessage, nil)
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