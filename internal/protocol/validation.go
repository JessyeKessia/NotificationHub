package protocol

// importação do pacote de erros para criar mensagens de erro personalizadas
import (
	"errors"
)

// funcao que valida o envelope recebido por um cliente ou servidor
func ValidateEnvelop(env Envelop) error {

	// se o tipo da mensagem estiver vazio, 
	// retorna um erro indicando que o tipo é obrigatório	
	if env.Type == "" {
		return errors.New("O tipo da mensagem é obrigatório")
	}

	// se o tipo da mensagem for diferente de "ping" e o tópico estiver vazio,
	// retorna um erro indicando que o tópico é obrigatório
	// O ping é importante para manter a conexão ativa, e não requer um tópico específico,
	// por isso, a validação do tópico é ignorada para mensagens do tipo "ping"!!!
	if env.Topic == "" && env.Type != "ping" {
		return errors.New("O tópico é obrigatório")
	}

	// se o tipo da mensagem for "publish" e o payload estiver vazio,
	// retorna um erro indicando que o payload é obrigatório para mensagens de publicação
	if env.Type == "publish" && env.Payload == nil {
		return errors.New("O payload é obrigatório para mensagens de publicação")
	}

	// se todas as validações passarem, retorna nil (null) indicando que o envelope é válido
	return nil
}