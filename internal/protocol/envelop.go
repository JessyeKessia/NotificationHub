package protocol

// classe que encapsula as mensagens enviadas entre cliente e servidor,
// contendo o tipo da mensagem, o tópico relacionado, o conteúdo principal, 
// um identificador de requisição para rastreamento e um campo de erro para mensagens de erro
type Envelop struct {
	Type      string      `json:"type"`
	Topic     string      `json:"topic,omitempty"`
	Payload   interface{} `json:"payload,omitempty"`
	RequestID string      `json:"request_id,omitempty"`
	Error     string      `json:"error,omitempty"`
}