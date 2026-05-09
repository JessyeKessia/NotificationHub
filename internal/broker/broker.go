package broker

import (
	"log"
	"sync"

	"notificationhub/internal/protocol"
)

// broker principal
// controla:
// - clientes conectados
// - tópicos

type Broker struct {

	Clients map[*Client]bool

	Topics map[string]*Topic

	Mutex sync.RWMutex
}

func NewBroker() *Broker {

	return &Broker{
		Clients: make(map[*Client]bool),
		Topics:  make(map[string]*Topic),
	}
}

// registra cliente conectado
func (b *Broker) Register(client *Client) {

	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	b.Clients[client] = true

	log.Println("cliente registrado:", client.ID)
}

// remove cliente conectado
func (b *Broker) Unregister(client *Client) {

	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	delete(b.Clients, client)

	// remove cliente de todos os tópicos
	for topicName := range client.Topics {

		if topic, exists := b.Topics[topicName]; exists {

			delete(topic.Subscribers, client)

			// remove tópico vazio
			if len(topic.Subscribers) == 0 {
				close(topic.Queue)
				delete(b.Topics, topicName)

				log.Println("tópico removido:", topicName)
			}
		}
	}

	close(client.Send)
	close(client.Close)

	log.Println("cliente removido:", client.ID)
}

// inscrição em tópico
func (b *Broker) Subscribe(client *Client, topicName string) {

	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	// cria tópico dinamicamente
	if _, exists := b.Topics[topicName]; !exists {

		topic := &Topic{
			Name:        topicName,
			Subscribers: make(map[*Client]bool),
			Queue:       make(chan protocol.Envelope, 100),
		}

		topic.StartWorker()

		b.Topics[topicName] = topic

		log.Println("tópico criado:", topicName)
	}

	topic := b.Topics[topicName]

	topic.Subscribers[client] = true

	client.Topics[topicName] = true

	client.SendJSON(protocol.Envelope{
		Type:  "ack",
		Topic: topicName,
	})
}

// remoção de inscrição
func (b *Broker) Unsubscribe(client *Client, topicName string) {

	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	topic, exists := b.Topics[topicName]

	if !exists {
		return
	}

	delete(topic.Subscribers, client)

	delete(client.Topics, topicName)

	// remove tópico vazio
	if len(topic.Subscribers) == 0 {

		close(topic.Queue)

		delete(b.Topics, topicName)

		log.Println("tópico removido:", topicName)
	}

	client.SendJSON(protocol.Envelope{
		Type:  "ack",
		Topic: topicName,
	})
}

// publicação em tópico
func (b *Broker) Publish(env protocol.Envelope) {

	b.Mutex.RLock()
	defer b.Mutex.RUnlock()

	topic, exists := b.Topics[env.Topic]

	// descarta mensagem sem inscritos
	if !exists || len(topic.Subscribers) == 0 {

		log.Println("mensagem descartada sem inscritos:", env.Topic)

		return
	}

	// adiciona mensagem na fila do tópico
	select {
	case topic.Queue <- env:
	default:
		log.Println("fila do tópico cheia:", env.Topic)
	}
}