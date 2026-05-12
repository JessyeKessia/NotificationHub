package broker

import (
	"net/http"
	"github.com/gorilla/websocket"
	"time"
	"log"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Permite todas as origens
	},
}

func (b *Broker) ServeWS(w http.ResponseWriter, r *http.Request) {
	// conexão WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)

	// trata erro de upgrade
	if err != nil {
		log.Println("websocket upgrade com erro:", err)
		return
	}

	client := &Client{
		ID:   r.RemoteAddr,
		Conn: conn,
		Send: make(chan []byte, 256),
		LastPong: time.Now(),
	}

	b.Mutex.Lock()
	b.Clients[client.ID] = client
	b.Mutex.Unlock()

	go client.writePump()
	go client.readPump(b)
}

func (b *Broker) ServePeer(w http.ResponseWriter, r *http.Request) {
	// conexão WebSocket para peer
	conn, err := upgrader.Upgrade(w, r, nil)

	if err != nil {
		log.Println("websocket upgrade para peer com erro:", err)
		return
	}

	peerAddr := r.RemoteAddr
	peer := &Peer{
		Addr: peerAddr,
		Conn: conn,
	}

	b.Mutex.Lock()
	b.Peers[peerAddr] = peer
	b.Mutex.Unlock()

	log.Println("peer aceito (inbound):", peerAddr)

	// escuta mensagens do peer
	go b.listenPeer(peer)
}