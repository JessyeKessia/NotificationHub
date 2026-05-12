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

	for _, topic := range topics {
		currentTopic := topic

		err := client.Subscribe(currentTopic, func(msg pubsub.Message) {
			log.Printf("[SUBSCRIBER] recebido topic=%s payload=%v", msg.Topic, msg.Payload)
		})
		if err != nil {
			log.Println("[TESTE] erro ao assinar:", err)
		}
	}

	time.Sleep(1 * time.Second)

	for _, topic := range topics {
		err := client.Publish(topic, map[string]any{
			"event": "teste de distribuição por hash",
			"topic": topic,
		})

		if err != nil {
			log.Println("[TESTE] erro ao publicar:", err)
		}

		time.Sleep(500 * time.Millisecond)
	}

	time.Sleep(3 * time.Second)
}