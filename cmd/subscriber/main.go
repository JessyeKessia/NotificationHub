package main

import (
	"log"
	"os"
	"os/signal"

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
			log.Printf("[SUBSCRIBER] recebido topic='%s' payload='%v'", msg.Topic, msg.Payload)
		})

		if err != nil {
			log.Println("[SUBSCRIBER] erro ao assinar tópico:", err)
		}
	}

	log.Println("[SUBSCRIBER] aguardando mensagens. Pressione Ctrl+C para sair.")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	<-stop

	log.Println("[SUBSCRIBER] encerrando...")
}