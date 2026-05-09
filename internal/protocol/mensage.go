package protocol
// mensagens que são enviadas entre cliente e servidor, 
// encapsulando os dados necessários para cada tipo de operação 

// publicacao no topico, 
// contendo o nome do topico e o conteudo principal da mensagem a ser publicada
type PublishMessage struct {
	Topic   string      `json:"topic"`
	Payload interface{} `json:"payload"`
}

// assinatura no topico
type SubscribeMessage struct {
	Topic string `json:"topic"`
}

// desinscrição no topico
type UnsubscribeMessage struct {
	Topic string `json:"topic"`
}

// resposta de confirmação da operação, como publicação ou assinatura
type AckMessage struct {
	Status    string `json:"status"`
	RequestID string `json:"request_id"`
}

// mensagem de erro caso ocorra algum problema durante a operação, 
// como falha na publicação ou assinatura
type ErrorMessage struct {
	Error     string `json:"error"`
	RequestID string `json:"request_id"`
}