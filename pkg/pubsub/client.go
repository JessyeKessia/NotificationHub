package pubsub

import "github.com/gorilla/websocket"

type Client struct {
	Conn *websocket.Conn
}

func Connect(url string) *Client {
	conn, _, _ := websocket.DefaultDialer.Dial(url, nil)
	return &Client{Conn: conn}
}

func (c *Client) Publish(topic string, payload interface{}) {
	c.Conn.WriteJSON(map[string]interface{}{
		"type": "publish",
		"topic": topic,
		"payload": payload,
	})
}