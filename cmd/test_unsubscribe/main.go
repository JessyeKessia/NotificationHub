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

	sub, _ := pubsub.NewClient(brokers)
	defer sub.Close()

	pub, _ := pubsub.NewClient(brokers)
	defer pub.Close()

	sub.Subscribe("cancelamentos", func(msg pubsub.Message) {
		log.Printf("[SUBSCRIBER] recebeu antes do unsubscribe: %v", msg.Payload)
	})

	time.Sleep(1 * time.Second)

	pub.Publish("cancelamentos", map[string]any{
		"message": "essa deve chegar",
	})

	time.Sleep(1 * time.Second)

	log.Println("[TESTE] Fazendo unsubscribe")
	sub.Unsubscribe("cancelamentos")

	time.Sleep(1 * time.Second)

	log.Println("[TESTE] Publicando após unsubscribe")
	pub.Publish("cancelamentos", map[string]any{
		"message": "essa deve ser descartada",
	})

	time.Sleep(3 * time.Second)
}