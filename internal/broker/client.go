package broker

import (
	"github.com/gorilla/websocket"
	"encoding/json"
	"log"
	"notificationhub/internal/protocol"

)

// representa um cliente conectado ao broker
type Client struct {

	// id identificador único do cliente conectado
	ID string

	// conexao WebSocket do cliente
	Conn *websocket.Conn

	// fila assíncrona de saída
	Send chan []byte

	// conjunto de topicos aos quais o cliente está inscrito
	Topics map[string]bool

	// referência ao broker para permitir interação com o broker
	Broker *Broker

	// sinalizador de fechamento do cliente
	Close chan struct{}
}
func (c *Client) ReadPump() {

	// fecha conexão ao encerrar
	defer c.Conn.Close()

	for {

		// le as mensagens websocket
		_, message, err := c.Conn.ReadMessage()

		// cliente desconectou
		if err != nil {
			log.Println("erro ao ler mensagem:", err)
			break
		}

		// estrutura para armazenar a mensagem decodificada
		var env protocol.Envelop

		// converte json → struct go (objeto go) 
		err = json.Unmarshal(message, &env)

		// se tiver erro na conversão, manda mensagem de erro
		if err != nil {

			log.Println("json inválido:", err)

			// envia erro padronizado
			errorResponse := protocol.NewError("", "payload inválido", )

			// converte struct go (objeto go) → json para enviar ao cliente
			data, _ := json.Marshal(errorResponse)

			// envia
			c.Send <- data

			continue
		}

		// valida mensagem de acordo com as regras de validação definidasd
		err = protocol.ValidateEnvelop(env)

		// erro de validacao
		if err != nil {

			log.Println("erro de validação:", err)

			errorResponse := protocol.NewError(
				env.RequestID,
				err.Error(),
			)

			data, _ := json.Marshal(errorResponse)

			c.Send <- data

			continue
		}

		// processa tipos do protocolo
		switch env.Type {

		case "publish":

			log.Println(
				"mensagem publicada no tópico:",
				env.Topic,
			)

			log.Println("payload:", env.Payload)

			ack := protocol.NewAck(env.RequestID)

			data, _ := json.Marshal(ack)

			c.Send <- data

		case "subscribe":

			// adiciona tópico ao cliente
			c.Topics[env.Topic] = true

			log.Println(
				"cliente inscrito no tópico:",
				env.Topic,
			)

			ack := protocol.NewAck(env.RequestID)

			data, _ := json.Marshal(ack)

			c.Send <- data

		case "unsubscribe":

			// remove inscrição
			delete(c.Topics, env.Topic)

			log.Println(
				"cliente removido do tópico:",
				env.Topic,
			)

			ack := protocol.NewAck(env.RequestID)

			data, _ := json.Marshal(ack)

			c.Send <- data

		case "ping":

			log.Println("ping recebido")

		default:

			errorResponse := protocol.NewError(
				env.RequestID,
				"tipo de mensagem desconhecido",
			)

			data, _ := json.Marshal(errorResponse)

			c.Send <- data
		}
	}
}