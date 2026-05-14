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

	sub.Subscribe("topico_quente", func(msg pubsub.Message) {
		log.Printf("[SUBSCRIBER-LENTO] recebeu: %v", msg.Payload)

		// Simula processamento lento.
		time.Sleep(500 * time.Millisecond)
	})

	time.Sleep(1 * time.Second)

	log.Println("[TESTE] Enviando rajada de mensagens para tópico quente")

	for i := 1; i <= 100; i++ {
		err := pub.Publish("topico_quente", map[string]any{
			"seq": i,
		})

		if err != nil {
			log.Println("[PUBLISHER] erro:", err)
		}
	}

	time.Sleep(10 * time.Second)
}