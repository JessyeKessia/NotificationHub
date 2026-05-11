package models

type Message struct {
	Topic     string                 `json:"topic"`
	Payload   map[string]interface{} `json:"payload"`
	RequestID string                 `json:"requestId"`
}