package broker

import (
	"log"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{

	CheckOrigin: func(r *http.Request) bool {

		return true
	},
}

// endpoint websocket
func ServeWS(
	broker *Broker,
	w http.ResponseWriter,
	r *http.Request,
) {

	conn, err := upgrader.Upgrade(
		w,
		r,
		nil,
	)

	if err != nil {

		log.Println(err)

		return
	}

	client := &Cliente{

		ID: uuid.NewString(),

		Conn: conn,

		Send: make(
			chan []byte,
			256,
		),

		Topics: make(
			map[string]bool,
		),

		Broker: broker,

		Close: make(chan struct{}),
	}

	broker.Register(client)

	go client.WritePump()

	go client.ReadPump()

	log.Println(
		"cliente conectado:",
		client.ID,
	)
}