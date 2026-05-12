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

	subscriber, err := pubsub.NewClient(brokers)
	if err != nil {
		log.Fatal(err)
	}
	defer subscriber.Close()

	publisher, err := pubsub.NewClient(brokers)
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()

	err = subscriber.Subscribe("alerta_rota", func(msg pubsub.Message) {
		log.Printf("[SUBSCRIBER] recebido topic=%s payload=%v", msg.Topic, msg.Payload)
	})
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	log.Println("[TESTE] Publicando no tópico alerta_rota")

	err = publisher.Publish("alerta_rota", map[string]any{
		"message": "Ônibus atrasado 10 minutos",
		"route":   "IFPB-JP",
	})
	if err != nil {
		log.Println("[TESTE] erro ao publicar:", err)
	}

	time.Sleep(3 * time.Second)
}