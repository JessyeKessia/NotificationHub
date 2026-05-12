# NotificationHub
Middleware Pub/Sub distribuído utilizando WebSocket.

## Objetivo

Permitir comunicação assíncrona entre publishers e subscribers através de tópicos.

### Pré-requisitos
- Go 1.18+ instalado
- Git

## Arquitetura
## Arquitetura


- Brokers WebSocket
- Hash de tópicos para distribuição
- Comunicação persistente
- Escalabilidade horizontal

## Instalação

1. **Clone o repositório**

```bash
git clone https://github.com/JessyeKessia/NotificationHub.git
cd NotificationHub
```

2. **Instale as dependências**

```bash
go mod download
go mod tidy
```

3. **Executando o servidor**

```bash
go run ./cmd/broker/main.go
```

4. **Acessando o projeto**
```bash
ws://localhost:8080/noticiationhub
```
5. **Testando mensagens no servidor**
```json
{
	"type":"subscribe",
	"topic":"noticia"
}
```
## Tecnologias

- Go
- Gorilla WebSocket
- JSON
- Goroutines e Channels

## Implmentar o projeto