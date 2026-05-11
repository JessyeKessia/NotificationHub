package broker

import (
	"net/http"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{}

func (b *Broker) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, _ := upgrader.Upgrade(w, r, nil)

	client := &Client{
		ID:   r.RemoteAddr,
		Conn: conn,
		Send: make(chan []byte, 256),
	}

	b.Clients[client.ID] = client

	go client.writePump()
	go client.readPump(b)
}