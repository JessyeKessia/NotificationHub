package main

import (
	"net/http"
	"notificationhub/internal/broker"
)

func main() {
	b := broker.NewBroker()

	http.HandleFunc("/notificationhub", b.ServeWS)

	go b.StartHeartbeat()

	http.ListenAndServe(":8080", nil)
}