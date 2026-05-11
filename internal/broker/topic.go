package broker

import (
	"encoding/json"
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

	Broker *Broker
}

func NewTopic(name string, b *Broker) *Topic {
	t := &Topic{
		Name: name,
		Subscribers: make(map[*Client]bool),
		Queue: make(chan protocol.Envelop, 100),
		Broker: b,
	}
	go t.dispatch()
	return t
}

func (t *Topic) dispatch() {
	for msg := range t.Queue {
		if len(t.Subscribers) == 0 {
			continue
		}

		data, _ := json.Marshal(msg)
		for c := range t.Subscribers {
			select {
			case c.Send <- data:
			default:
			}
		}
	}
}

func (t *Topic) checkEmpty() {
	if len(t.Subscribers) == 0 {
		delete(t.Broker.Topics, t.Name)
	}
}