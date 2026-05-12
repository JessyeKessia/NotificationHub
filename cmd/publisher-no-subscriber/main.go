package main

import (
	"log"

	"notificationhub/pkg/pubsub"
)

func main() {
	client, err := pubsub.NewClient([]string{
		"ws://localhost:8081/notificationhub",
		"ws://localhost:8082/notificationhub",
		"ws://localhost:8083/notificationhub",
	})

	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	err = client.Publish("topico_sem_assinantes", map[string]any{
		"message": "essa mensagem deve ser descartada",
	})

	if err != nil {
		log.Println("[PUBLISHER-NO-SUBSCRIBER] erro:", err)
	}
}