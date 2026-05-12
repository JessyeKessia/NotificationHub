package protocol

// mensagens que são enviadas entre cliente e servidor

// publicacao no topico,

// cria a mensagem de confirmação de operação, como publicação ou assinatura,
// contendo o status da operação e o identificador da requisição para rastreamento
func NewAck(requestID string) Envelop {
	return Envelop{
		Type:      "ack",
		RequestID: requestID,
	}
}

// função que cria um envelope de erro com o tipo "error",
// contendo o identificador da requisição e a mensagem de erro fornecida
func NewError(requestID, message string) Envelop {
	return Envelop{
		Type:      "error",
		RequestID: requestID,
		Error:     message,
	}
}