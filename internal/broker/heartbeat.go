package broker

import (
	"time"
	"github.com/gorilla/websocket"
)

func (b *Broker) StartHeartbeat() {

	ticker := time.NewTicker(30 * time.Second)

	for range ticker.C {

		b.Mutex.Lock()

		for id, client := range b.Clients {

			if time.Since(client.LastPong) > 60*time.Second {

				client.Conn.Close()
				delete(b.Clients, id)
				continue
			}

			err := client.Conn.WriteMessage(
				websocket.PingMessage,
				[]byte("ping"),
			)

			if err != nil {
				client.Conn.Close()
				delete(b.Clients, id)
			}
		}

		b.Mutex.Unlock()
	}
}