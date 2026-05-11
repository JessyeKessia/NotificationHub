package main

import (
	"net/http"
	"notificationhub/internal/broker"
	"os"
	"log"
	"time"
)

func main() {
	b := broker.NewBroker()

	http.HandleFunc("/notificationhub", b.ServeWS)
	http.HandleFunc("/federation", b.ServePeer)

	// inicia o servidor HTTP em background
	go http.ListenAndServe(":8080", nil)

	// aguarda o servidor iniciar completamente
	time.Sleep(3 * time.Second)

	// conecta peers
	peer := os.Getenv("PEER")

	if peer != "" {

		err := b.ConnectPeer(peer)

		if err != nil {

			log.Println(
				"erro ao conectar peer:",
				err,
			)
		}
	}

	// mantém o programa rodando
	select {}
}