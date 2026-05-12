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

	log.Println("[TESTE] Publicando em tópico sem subscribers")

	err = client.Publish("topico_sem_assinantes", map[string]any{
		"message": "essa mensagem deve ser descartada",
		"time":    time.Now().Format(time.RFC3339),
	})

	if err != nil {
		log.Println("[PUBLISHER-NO-SUBSCRIBER] erro:", err)
	}

	time.Sleep(2 * time.Second)
}