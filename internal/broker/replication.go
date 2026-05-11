package broker

import (
	"log"
	"github.com/gorilla/websocket"
)

// replica mensagens para outros brokers
func (b *Broker) Forward(
	topic string,
	msg []byte,
	origin string,
) {

	b.Mutex.RLock()
	peers := make(map[string]*Peer)
	for k, v := range b.Peers {
		peers[k] = v
	}

	b.Mutex.RUnlock()

	log.Printf("Forward: replicando para %d peers, origin=%s", len(peers), origin)

	for addr, peer := range peers {

		// evita loop infinito
		if addr == origin {
			log.Printf("Forward: ignorando peer %s (é a origem)", addr)
			continue
		}

		err := peer.Conn.WriteMessage(
			websocket.TextMessage,
			msg,
		)

		if err != nil {

			log.Println(
				"erro ao encaminhar para peer:",
				addr,
				"erro:",
				err,
			)

			continue
		}

		log.Printf("Forward: mensagem enviada com sucesso para peer %s", addr)
	}
}