package broker

import (
	"time"

	"github.com/gorilla/websocket"
)

func (b *Broker) StartHeartbeat() {

	ticker := time.NewTicker(30 * time.Second)

	for range ticker.C {

		b.Mutex.RLock()

		for _, client := range b.Clients {

			// verifica timeout
			if time.Since(client.LastPong) > 60*time.Second {

				client.Conn.Close()
				delete(b.Clients, client.ID)

				continue
			}

			// envia ping websocket
			err := client.Conn.WriteMessage(
				websocket.PingMessage,
				[]byte("ping"),
			)

			if err != nil {
				client.Conn.Close()
				delete(b.Clients, client.ID)
			}
		}

		b.Mutex.RUnlock()
	}
}