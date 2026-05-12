package main

import "notificationhub/pkg/pubsub"

func main() {
	c := pubsub.Connect("ws://localhost:8080/notificationhub")

	c.Publish("orders", map[string]any{
		"id": 1,
		"value": 100,
	})
}