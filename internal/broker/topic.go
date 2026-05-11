package broker

import (
	"encoding/json"
	"notificationhub/internal/protocol"
	"log"
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

		t.Broker.Mutex.RLock()

		subs := make([]*Client, 0, len(t.Subscribers))
		for c := range t.Subscribers {
			subs = append(subs, c)
		}

		t.Broker.Mutex.RUnlock()

		// AVISO QUANDO NÃO TEM NINGUÉM INSCRITO PARA RECEBER A MENSAGEM
		if len(subs) == 0 {
			log.Printf("tópico '%s' sem subscribers: mensagem descartada", t.Name)
			continue
		}

		data, _ := json.Marshal(msg)

		for _, c := range subs {
			select {
			case c.Send <- data:
			default:
				log.Printf("⚠️ cliente %s com buffer cheio, mensagem descartada", c.ID)
			}
		}
	}
}

func (t *Topic) checkEmpty() {

	t.Broker.Mutex.Lock()
	defer t.Broker.Mutex.Unlock()

	if len(t.Subscribers) == 0 {
		delete(t.Broker.Topics, t.Name)
	}
}