# NotificationHub
Middleware Pub/Sub distribuído utilizando WebSocket.

## Objetivo

Permitir comunicação assíncrona entre publishers e subscribers através de tópicos.

## Arquitetura

- Brokers WebSocket
- Hash de tópicos para distribuição
- Comunicação persistente
- Escalabilidade horizontal

## Tecnologias

- Go
- Gorilla WebSocket
- JSON
- Goroutines e Channels