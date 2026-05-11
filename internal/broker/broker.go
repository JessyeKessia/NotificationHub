package broker

import (
	"sync"
	"notificationhub/internal/models"
	"notificationhub/internal/protocol"
)

type Broker struct {
	Topics  map[string]*Topic
	Clients map[string]*Client
	Peers   map[string]*Peer
	Mutex   sync.RWMutex
}

func NewBroker() *Broker {
	return &Broker{
		Topics:  make(map[string]*Topic),
		Clients: make(map[string]*Client),
		Peers:   make(map[string]*Peer),
	}
}

func (b *Broker) Publish(msg models.Message) {
	b.Mutex.RLock()
	topic, ok := b.Topics[msg.Topic]
	b.Mutex.RUnlock()

	if !ok {
		return
	}

	env := protocol.Envelop{
		Type:    "message",
		Topic:   msg.Topic,
		Payload: msg.Payload,
	}

	select {
	case topic.Queue <- env:
	default:
	}
}