package main

import (
	"fmt"
	"notificationhub/pkg/pubsub"
)

func main() {
	c := pubsub.Connect("ws://localhost:8080/notificationhub")

	for {
		_, msg, _ := c.Conn.ReadMessage()
		fmt.Println(string(msg))
	}
}