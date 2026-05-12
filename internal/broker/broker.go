package broker

import (
	"notificationhub/internal/protocol"
	"log"
	"sync"
	"encoding/json"
)

type PublishStatus string

const(
	PublishStatusPublished             PublishStatus = "published"
	PublishStatusDiscardedNoSubscribers PublishStatus = "discarded_no_subscribers"
	PublishStatusQueueFull             PublishStatus = "topic_queue_full"
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

func (b *Broker) Publish(
	topic string,
	payload interface{},
	// origin é o endereço do broker que originou a mensagem, 
	// usado para evitar loops de replicação
	origin string,
) PublishStatus {
	b.Mutex.RLock()
	t, exists := b.Topics[topic]

	if !exists || len(t.Subscribers) == 0 {
		b.Mutex.RUnlock()
		log.Println("Tópico '%s' não existe ou não tem inscritos: mensagem descartada do", topic)
		return PublishStatusDiscardedNoSubscribers
	}

	envelop := protocol.Envelop{
		Type:    "message",
		Topic:   topic,
		Payload: payload,
	}

	// envia para a fila do tópico
	select {
	case t.Queue <- envelop:
		b.Mutex.RUnlock()
		log.Printf("[PUBSUB] mensagem publicada no tópico '%s' para %d subscriber(s)", topic, len(t.Subscribers))
	default:
		b.Mutex.RUnlock()
		log.Printf("[BACKPRESSURE] fila cheia no tópico '%s': mensagem rejeitada", topic)
		return PublishStatusQueueFull
	}

	if origin != "" {
		log.Printf("[FEDERATION] mensagem recebida de peer origem=%s", origin)
	}

	if len(b.Peers) > 0 {
	// replica para outros brokers
	data, _ := json.Marshal(envelop)
	b.Forward(topic, data, origin)

	
	}
	return PublishStatusPublished
}
func (b *Broker) Subscribe(
	topic string,
	client *Client,
) {

	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	t, exists := b.Topics[topic]

	if !exists {

		t = NewTopic(topic, b)

		b.Topics[topic] = t
	}

	t.Subscribers[client] = true

	log.Println("cliente inscrito no tópico:", topic)
}

func (b *Broker) Unsubscribe(
	topic string,
	client *Client,
) {

	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	t, exists := b.Topics[topic]

	if !exists {
		log.Println("Tópico não existe: mensagem ignorada")
		return
	}

	delete(t.Subscribers, client)

	log.Printf("[UNSUBSCRIBE] cliente %s removido do tópico '%s'", client.ID, topic)

	if len(t.Subscribers) == 0 {
		delete(b.Topics, topic)
		close(t.Queue)

		log.Printf("[TOPIC] tópico removido por falta de subscribers: %s", topic) }
}