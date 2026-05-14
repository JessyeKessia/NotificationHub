package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"notificationhub/internal/broker"
)

func main() {
	b := broker.NewBroker()

	brokerID := os.Getenv("BROKER_ID")
	if brokerID == "" {
		brokerID = "broker-local"
	}

	peer := os.Getenv("PEER")

	http.HandleFunc("/notificationhub", b.ServeWS)
	http.HandleFunc("/federation", b.ServePeer)

	log.Printf("[BROKER] id=%s iniciando na porta interna 8080", brokerID)
	log.Printf("[BROKER] endpoint clientes: /notificationhub")
	log.Printf("[BROKER] endpoint federação: /federation")

	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatal("[BROKER] erro ao iniciar servidor:", err)
		}
	}()

	// Espera curta para o servidor local subir antes de tentar conectar ao peer.
	time.Sleep(2 * time.Second)

	if peer != "" {
		log.Printf("[FEDERATION] %s tentando conectar ao peer %s", brokerID, peer)

		err := b.ConnectPeer(peer)
		if err != nil {
			log.Printf("[FEDERATION] %s não conseguiu conectar ao peer %s: %v", brokerID, peer, err)
		}
	}

	select {}
}