package broker

import (
	"github.com/gorilla/websocket"
	"time"
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
	c.Conn.SetPongHandler(func(string) error {
		c.LastPong = time.Now()
		return nil
	})

	for {
		_, msg, err := c.Conn.ReadMessage()
		if err != nil {
			return
		}
		_ = msg
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