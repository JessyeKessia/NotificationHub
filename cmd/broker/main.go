package main

// importes necessárias para o funcionamento do servidor WebSocket,
import (
	"log"
	"net/http"
	// pacote Gorilla WebSocket para facilitar a implementação do servidor WebSocket
	"github.com/gorilla/websocket"
)

// estrutura que representa um cliente conectado ao servidor WebSocket,
// contendo um identificador único, a conexão WebSocket, um canal para enviar mensagens,
// um mapa de tópicos aos quais o cliente está inscrito e um canal para sinalizar o fechamento da conexão
var upgrader = websocket.Upgrader{
	// permite conexoes
	CheckOrigin: func(r *http.Request) bool {
		// aceite conexão de QUALQUER origem
		return true
	},
}

// função que lida com as conexões WebSocket dos clientes,
// atualizando a conexão para o protocolo WebSocket, 
// lendo mensagens dos clientes e logando as mensagens recebidas
func wsHandler(w http.ResponseWriter, r *http.Request) {

	// pego a conexao do cliente e atualizo para o protocolo WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)

	// se ocorrer algum erro durante a atualização,
	// o erro retorna para o cliente
	if err != nil {
		log.Println(err)
		return
	}

	// garante que a conexão será fechada quando finalizar
	defer conn.Close()

	// confirma que um cliente se conectou
	log.Println("Cliente conectado")

	// ler as mensagens enviadas pelo cliente em um loop infinito,
	for {
		// pega a mensagem e o erro
		_, msg, err := conn.ReadMessage()
		// se tiver erro, sai do loop e fecha a conexao
		if err != nil {
			log.Println(err)
			break
		}
		// printa as mensagens recebidas
		log.Println(string(msg))
	}
}
// avisa que o broker está rodando na porta 8080 e 
// inicia o servidor HTTP para lidar com as conexões WebSocket dos clientes
func main() {

	// define rota /ws para lidar com as conexoes websocket
	http.HandleFunc("/ws", wsHandler)

	log.Println("Broker rodando na porta 8080")
	// escutando na porta 8080
	http.ListenAndServe(":8080", nil)
}