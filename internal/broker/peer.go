package broker

import "github.com/gorilla/websocket"

// representa outro broker conectado
type Peer struct {

	// endereço do broker remoto
	Addr string

	// conexão websocket
	Conn *websocket.Conn
}