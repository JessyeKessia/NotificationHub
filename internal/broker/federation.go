package broker

import "github.com/gorilla/websocket"

type Peer struct {
	// endereço do peer do outro brocker
	Addr string
	// conexão WebSocket para comunicação entre brokers
	Conn *websocket.Conn
}

func (b *Broker) Forward(topic string, msg []byte, origin string) {
	for addr, peer := range b.Peers {
		if addr == origin {
			continue // evita loop
		}
		peer.Conn.WriteMessage(websocket.TextMessage, msg)
	}
}