package broker

import (
	"encoding/json"
	"log"

	"notificationhub/internal/protocol"
)

// representa um tópico do broker
// possui:
// - nome
// - inscritos
// - fila de mensagens

type Topic struct {
	// nome do tópico
	Name string

	// os clientes inscritos nesse tópico
	Subscribers map[*Client]bool

	// fila com buffer com as mensagens a serem distribuídas para os inscritos
	Queue chan protocol.Envelop
}

// worker responsável pela distribuição de mensagens
func (t *Topic) StartWorker() {

	go func() {

		for message := range t.Queue {

			data, err := json.Marshal(message)

			if err != nil {
				continue
			}

			for client := range t.Subscribers {
				select {
					case client.Send <- data:
					default:
						log.Println("fila cheia para cliente", client.ID)
						
					}
			}
		}
	}()
}