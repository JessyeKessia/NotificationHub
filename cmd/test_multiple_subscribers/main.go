package main

import (
	"log"
	"time"

	"notificationhub/pkg/pubsub"
)

func main() {
	brokers := []string{
		"ws://localhost:8081/notificationhub",
		"ws://localhost:8082/notificationhub",
		"ws://localhost:8083/notificationhub",
	}

	sub1, _ := pubsub.NewClient(brokers)
	defer sub1.Close()

	sub2, _ := pubsub.NewClient(brokers)
	defer sub2.Close()

	pub, _ := pubsub.NewClient(brokers)
	defer pub.Close()

	sub1.Subscribe("notificacoes", func(msg pubsub.Message) {
		log.Printf("[SUBSCRIBER-1] recebeu: %v", msg.Payload)
	})

	sub2.Subscribe("notificacoes", func(msg pubsub.Message) {
		log.Printf("[SUBSCRIBER-2] recebeu: %v", msg.Payload)
	})

	time.Sleep(1 * time.Second)

	pub.Publish("notificacoes", map[string]any{
		"message": "mensagem deve chegar para dois subscribers",
	})

	time.Sleep(3 * time.Second)
}