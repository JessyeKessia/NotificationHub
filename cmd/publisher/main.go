package main

import (
	"log"
	"time"
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

	topics := []string{
		"localizacao_onibus",
		"reserva_criada",
		"reserva_cancelada",
		"alerta_rota",
	}

	for i := 1; i <= 10; i++ {
		for _, topic := range topics {
			payload := map[string]any{
				"id":        i,
				"topic":     topic,
				"message":   "evento de teste do NotificationHub",
				"timestamp": time.Now().Format(time.RFC3339),
			}

			if err := client.Publish(topic, payload); err != nil {
				log.Println("[PUBLISHER] erro ao publicar:", err)
			}

			time.Sleep(500 * time.Millisecond)
		}
	}
}