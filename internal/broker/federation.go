package broker

import (
	"encoding/json"
	"log"

	"notificationhub/internal/protocol"

	"github.com/gorilla/websocket"
)

// conecta em outro broker do cluster
func (b *Broker) ConnectPeer(
	addr string,
) error {

	conn, _, err := websocket.DefaultDialer.Dial(
		addr,
		nil,
	)

	if err != nil {
		return err
	}

	peer := &Peer{
		Addr: addr,
		Conn: conn,
	}

	b.Mutex.Lock()
	b.Peers[addr] = peer
	b.Mutex.Unlock()

	log.Println("peer conectado:", addr)

	// inicia escuta do peer
	go b.listenPeer(peer)

	return nil
}

// escuta mensagens vindas de outros brokers
func (b *Broker) listenPeer(
	peer *Peer,
) {

	for {

		_, data, err := peer.Conn.ReadMessage()

		if err != nil {

			log.Println("peer desconectado:", peer.Addr)

			b.Mutex.Lock()
			delete(b.Peers, peer.Addr)
			b.Mutex.Unlock()

			return
		}

		var env protocol.Envelop

		err = json.Unmarshal(data, &env)

		if err != nil {
			continue
		}

		// Garante que temos conexão reversa com este peer
		b.ensureReversePeerConnection(peer.Addr)

		// redistribui localmente
		b.Publish(
			env.Topic,
			env.Payload,
			nil,
			peer.Addr,
		)
	}
}

// garante que existe uma conexão reversa com o peer (para replicação bidirecional)
func (b *Broker) ensureReversePeerConnection(peerAddr string) {

	b.Mutex.RLock()
	_, exists := b.Peers[peerAddr]
	b.Mutex.RUnlock()

	if exists {
		return // já temos uma conexão com este peer
	}

	// tenta conectar de volta neste peer
	err := b.ConnectPeer(peerAddr)
	if err != nil {
		log.Println("erro ao conectar reverso em", peerAddr, ":", err)
	}
}