# NotificationHub

Middleware **Publish/Subscribe distribuído** desenvolvido em **Go**, utilizando **WebSocket** para comunicação persistente entre clientes e brokers.

A ideia central do projeto é permitir que aplicações publicadoras (*publishers*) enviem mensagens para tópicos, enquanto aplicações consumidoras (*subscribers*) recebem apenas as mensagens dos tópicos nos quais estão inscritas.

Este projeto foi desenvolvido para a disciplina de **Programação Distribuída**, com foco em comunicação assíncrona, brokers, tópicos, balanceamento por hash, uso de WebSocket, goroutines e channels.

---

## 1. Objetivo do projeto

O objetivo do NotificationHub é implementar um middleware Pub/Sub capaz de:

- aceitar conexões de clientes via WebSocket;
- permitir que clientes publiquem mensagens em tópicos;
- permitir que clientes se inscrevam em tópicos;
- permitir que clientes removam inscrição de tópicos;
- criar tópicos dinamicamente quando houver inscrição;
- remover tópicos quando não houver mais subscribers;
- descartar mensagens publicadas em tópicos sem subscribers e avisar o publicador;
- bufferizar mensagens para evitar bloqueio direto entre publicação e entrega;
- distribuir tópicos entre múltiplos brokers usando hash consistente;
- manter comunicação entre brokers por meio de uma camada de federação.

Em termos simples: o publisher não precisa saber quem vai consumir a mensagem, e o subscriber não precisa saber quem publicou. Ambos conhecem apenas o nome do tópico.

---

## 2. Visão geral da arquitetura

A arquitetura do projeto foi organizada em três partes principais:

```text
Aplicações de exemplo
        |
        | usam a biblioteca pkg/pubsub
        v
Biblioteca Cliente Pub/Sub
        |
        | escolhe o broker usando hash do tópico
        v
Brokers WebSocket
        |
        | entregam mensagens aos subscribers locais
        v
Tópicos com filas bufferizadas
```

Além disso, os brokers também possuem um endpoint de federação:

```text
Broker 1 <---- /federation ----> Broker 2 <---- /federation ----> Broker 3
```

A federação foi mantida como uma camada complementar de comunicação entre brokers. A estratégia principal de balanceamento, porém, é o **hash consistente por tópico**, feito na biblioteca cliente.

---

## 3. Decisões de design tomadas

### 3.1 Uso de WebSocket

O WebSocket foi escolhido porque o middleware precisa manter comunicação contínua entre clientes e brokers.

Diferente de uma requisição HTTP comum, no WebSocket a conexão permanece aberta. Isso permite que o broker envie mensagens para o subscriber assim que elas estiverem disponíveis, sem o cliente ficar perguntando repetidamente se há novas mensagens.

No projeto existem dois endpoints principais:

```text
/notificationhub -> usado por publishers e subscribers
/federation      -> usado para comunicação entre brokers
```

---

### 3.2 Publish/Subscribe com tópicos

A comunicação é baseada em tópicos.

Exemplo:

```text
Tópico: alerta_rota
Mensagem: { "mensagem": "Ônibus atrasado 10 minutos" }
```

Quem publica não sabe quem está inscrito. Quem consome apenas se inscreve nos tópicos de interesse.

Isso reduz o acoplamento entre processos distribuídos.

---

### 3.3 Hash consistente para balanceamento

A estratégia principal de balanceamento é o uso de **hash consistente**.

A biblioteca cliente recebe uma lista de brokers disponíveis e, para cada tópico, decide qual broker será responsável por ele.

Exemplo conceitual:

```text
localizacao_onibus -> broker1
reserva_criada     -> broker2
reserva_cancelada  -> broker3
alerta_rota        -> broker1
```

Isso significa que o código da aplicação não precisa escolher manualmente o broker. A aplicação apenas chama:

```go
client.Publish("alerta_rota", payload)
client.Subscribe("alerta_rota", handler)
```

Internamente, a biblioteca calcula o destino usando o hash do tópico.

Essa decisão atende ao requisito de esconder do cliente o fato de que existem múltiplos brokers.

---

### 3.4 Federação entre brokers

O projeto também possui uma camada de federação entre brokers.

Essa camada permite que brokers mantenham conexões WebSocket entre si, usando o endpoint:

```text
/federation
```

No Docker Compose, os brokers são conectados em formato de anel:

```text
broker1 -> broker2 -> broker3 -> broker1
```

É importante deixar claro: a federação não é o mecanismo principal de balanceamento. O balanceamento principal é feito por hash na biblioteca cliente.

A federação foi mantida como uma extensão arquitetural para comunicação entre brokers, podendo servir como base futura para replicação de metadados, monitoramento entre nós ou estratégias mais completas de tolerância a falhas.

---

### 3.5 Filas bufferizadas por tópico

Cada tópico possui uma fila interna.

Quando uma mensagem é publicada, ela não é enviada diretamente para cada subscriber dentro da mesma execução da publicação. Primeiro, ela entra na fila do tópico. Depois, uma goroutine do tópico distribui essa mensagem para os subscribers.

Isso evita que o broker fique bloqueado entregando uma mensagem enquanto novas mensagens chegam.

Esse desenho usa dois conceitos importantes de Go:

- **goroutines**, para execução concorrente;
- **channels**, para comunicação segura entre partes do sistema.

---

### 3.6 Backpressure e mensagens descartadas

Se uma mensagem for publicada em um tópico sem subscribers, ela é descartada e o publicador recebe um aviso.

Isso é importante porque o projeto não tem persistência em banco de dados ou disco. Logo, não faria sentido armazenar mensagens que ninguém vai consumir.

Também existe tratamento para fila cheia. Se um tópico estiver sobrecarregado, o broker pode rejeitar a mensagem e informar o publicador.

Esse comportamento protege o broker contra crescimento indefinido de memória.

---

## 4. Estrutura de pastas e arquivos

A estrutura atual do projeto é:

```text
NotificationHub/
│   broker
│   docker-compose.yml
│   dockerfile
│   go.mod
│   go.sum
│   README.md
│
├───cmd
│   ├───broker
│   │       main.go
│   │
│   ├───publisher
│   │       main.go
│   │
│   └───subscriber
│           main.go
│
├───internal
│   ├───broker
│   │       broker.go
│   │       client.go
│   │       federation.go
│   │       heartbeat.go
│   │       peer.go
│   │       replication.go
│   │       server.go
│   │       topic.go
│   │
│   └───protocol
│           envelop.go
│           responses.go
│
└───pkg
    │   hash.go
    │
    └───pubsub
            client.go
```

---

## 5. Explicação de cada pasta

### `cmd/`

Contém os programas executáveis do projeto.

Cada subpasta dentro de `cmd` representa uma aplicação que pode ser executada com `go run`.

---

### `cmd/broker/`

Contém o executável principal do broker.

O arquivo `main.go` inicia o broker, registra os endpoints WebSocket e conecta o broker a um peer, caso a variável de ambiente `PEER` esteja configurada.

Responsabilidades:

- iniciar o servidor WebSocket;
- expor `/notificationhub` para clientes;
- expor `/federation` para outros brokers;
- ler configurações por variáveis de ambiente;
- manter o processo do broker rodando.

---

### `cmd/publisher/`

Contém uma aplicação de exemplo que publica mensagens em tópicos.

Ela usa a biblioteca `pkg/pubsub`, e não acessa diretamente a implementação interna do broker.

Responsabilidade:

- simular uma aplicação publicadora;
- enviar mensagens para um ou mais tópicos;
- demonstrar o uso da função `Publish`.

---

### `cmd/subscriber/`

Contém uma aplicação de exemplo que consome mensagens de tópicos.

Responsabilidade:

- simular uma aplicação consumidora;
- se inscrever em tópicos;
- receber mensagens publicadas;
- demonstrar o uso da função `Subscribe`.

---

### `internal/`

Contém código interno do sistema.

Em Go, a pasta `internal` indica que esses pacotes não devem ser usados diretamente por aplicações externas. Eles fazem parte da implementação interna do NotificationHub.

---

### `internal/broker/`

Contém a lógica principal do broker.

É aqui que ficam as estruturas responsáveis por clientes, tópicos, filas, federação e entrega de mensagens.

---

### `internal/protocol/`

Contém as estruturas do protocolo de comunicação.

O protocolo define o formato das mensagens JSON trocadas via WebSocket.

Exemplo de mensagem:

```json
{
  "type": "publish",
  "topic": "alerta_rota",
  "payload": {
    "mensagem": "Ônibus atrasado 10 minutos"
  },
  "request_id": "req-1"
}
```

---

### `pkg/`

Contém código que pode ser usado por aplicações externas.

Neste projeto, a parte mais importante é a biblioteca cliente Pub/Sub.

---

### `pkg/pubsub/`

Contém a biblioteca cliente usada por publishers e subscribers.

Essa biblioteca esconde os detalhes de conexão WebSocket, roteamento por hash e escolha do broker.

A aplicação usa métodos simples, como:

```go
client.Publish("alerta_rota", payload)
client.Subscribe("alerta_rota", handler)
```

---

## 6. Explicação dos principais arquivos

### `internal/broker/broker.go`

Arquivo central do broker.

Responsabilidades:

- manter o mapa de tópicos;
- manter o mapa de clientes conectados;
- manter o mapa de peers conectados;
- processar publicações;
- processar inscrições;
- processar remoções de inscrição;
- descartar mensagens sem subscribers;
- encaminhar mensagens para federação quando aplicável.

---

### `internal/broker/client.go`

Representa um cliente conectado ao broker.

Responsabilidades:

- ler mensagens WebSocket recebidas do cliente;
- interpretar comandos `publish`, `subscribe` e `unsubscribe`;
- enviar respostas `ack` ou `error`;
- manter controle de ping/pong;
- enviar mensagens de saída pela conexão WebSocket.

---

### `internal/broker/topic.go`

Representa um tópico dentro do broker.

Responsabilidades:

- guardar os subscribers daquele tópico;
- manter uma fila bufferizada de mensagens;
- distribuir mensagens da fila para os subscribers;
- evitar que a publicação fique presa ao envio direto para cada cliente.

---

### `internal/broker/server.go`

Contém os handlers WebSocket do broker.

Responsabilidades:

- aceitar conexões de clientes em `/notificationhub`;
- aceitar conexões de brokers em `/federation`;
- criar estruturas `Client` e `Peer`;
- iniciar as goroutines de leitura e escrita.

---

### `internal/broker/federation.go`

Cuida da conexão ativa com outros brokers.

Responsabilidades:

- conectar em outro broker via WebSocket;
- registrar peers conectados;
- escutar mensagens vindas de outros brokers;
- redistribuir mensagens recebidas pela federação.

---

### `internal/broker/peer.go`

Define a estrutura que representa outro broker conectado.

Responsabilidades:

- armazenar o endereço do peer;
- armazenar a conexão WebSocket com esse peer.

---

### `internal/broker/replication.go`

Responsável por encaminhar mensagens para peers.

Responsabilidades:

- copiar a lista de peers conectados;
- enviar mensagens para outros brokers;
- evitar encaminhar a mensagem de volta para o broker de origem.

---

### `internal/broker/heartbeat.go`

Contém lógica de heartbeat.

Responsabilidades:

- verificar se clientes continuam vivos;
- enviar ping;
- fechar conexões inativas.

---

### `internal/protocol/envelop.go`

Define o envelope padrão das mensagens.

Campos principais:

```go
Type      string
Topic     string
Payload   interface{}
RequestID string
Error     string
```

---

### `internal/protocol/responses.go`

Contém funções auxiliares para criar respostas do protocolo.

Exemplos:

- `ack`, para confirmação;
- `error`, para erros.

---

### `pkg/hash.go`

Implementa o anel de hash consistente.

Responsabilidades:

- adicionar brokers ao anel;
- calcular qual broker atende determinado tópico;
- distribuir tópicos entre brokers de forma previsível.

---

### `pkg/pubsub/client.go`

Implementa a biblioteca cliente.

Responsabilidades:

- receber a lista de brokers disponíveis;
- escolher o broker correto usando hash do tópico;
- abrir conexões WebSocket;
- reutilizar conexões já abertas;
- publicar mensagens;
- se inscrever em tópicos;
- remover inscrições;
- receber mensagens do broker;
- chamar handlers definidos pela aplicação.

---

## 7. Protocolo de mensagens

As mensagens trocadas via WebSocket seguem um envelope JSON.

### Publicação

```json
{
  "type": "publish",
  "topic": "alerta_rota",
  "payload": {
    "mensagem": "Ônibus atrasado 10 minutos"
  },
  "request_id": "req-1"
}
```

### Inscrição

```json
{
  "type": "subscribe",
  "topic": "alerta_rota",
  "request_id": "req-2"
}
```

### Remoção de inscrição

```json
{
  "type": "unsubscribe",
  "topic": "alerta_rota",
  "request_id": "req-3"
}
```

### Mensagem entregue ao subscriber

```json
{
  "type": "message",
  "topic": "alerta_rota",
  "payload": {
    "mensagem": "Ônibus atrasado 10 minutos"
  }
}
```

### Confirmação

```json
{
  "type": "ack",
  "payload": "published",
  "request_id": "req-1"
}
```

### Erro

```json
{
  "type": "error",
  "error": "topic_queue_full",
  "request_id": "req-1"
}
```

---

## 8. Pré-requisitos

Para rodar o projeto, é recomendado ter instalado:

- Git;
- Go 1.22+;
- Docker;
- Docker Compose.

O projeto pode ser executado localmente com `go run`, mas a forma recomendada é usando Docker Compose, porque o sistema foi pensado para rodar múltiplos brokers.

---

## 9. Instalação

Clone o repositório:

```bash
git clone https://github.com/JessyeKessia/NotificationHub.git
cd NotificationHub
```

Baixe as dependências Go:

```bash
go mod download
go mod tidy
```

---

## 10. Executando com Docker Compose

Suba os brokers:

```bash
docker compose up --build
```

Ou, se quiser rodar em segundo plano:

```bash
docker compose up -d --build
```

Para acompanhar os logs:

```bash
docker compose logs -f
```

Para parar tudo:

```bash
docker compose down
```

---

## 11. Endpoints disponíveis

Com Docker Compose, os brokers ficam acessíveis em:

```text
broker1 -> ws://localhost:8081/notificationhub
broker2 -> ws://localhost:8082/notificationhub
broker3 -> ws://localhost:8083/notificationhub
```

A federação usa o endpoint interno:

```text
/federation
```

Exemplo dentro da rede Docker:

```text
ws://broker2:8080/federation
```

---

## 12. Executando sem Docker

Também é possível rodar um broker localmente:

```bash
go run ./cmd/broker
```

Por padrão, o broker sobe na porta configurada no código ou na variável de ambiente.

No PowerShell, para rodar vários brokers localmente, você pode usar:

```powershell
$env:BROKER_PORT="8081"
go run ./cmd/broker
```

Em outro terminal:

```powershell
$env:BROKER_PORT="8082"
go run ./cmd/broker
```

Porém, para a demonstração principal do projeto, a recomendação é usar Docker Compose.

---

## 13. Aplicações de exemplo

### Publisher

Executa uma aplicação que publica mensagens:

```bash
go run ./cmd/publisher
```

### Subscriber

Executa uma aplicação que se inscreve em tópicos e recebe mensagens:

```bash
go run ./cmd/subscriber
```

Para testar corretamente, deixe os brokers rodando com Docker Compose antes de executar publisher ou subscriber.

---

## 14. Testes críticos do sistema

Os testes críticos foram pensados para mostrar não apenas o caso feliz, mas também situações problemáticas que o broker precisa tratar.

Antes de rodar os testes, suba os brokers:

```bash
docker compose up --build
```

Em outro terminal, execute os testes.

---

### Teste 1 — Publicação sem subscribers

Objetivo:

Mostrar que uma mensagem publicada em um tópico sem subscribers é descartada, e o publicador é informado.

Comando:

```bash
go run ./cmd/test_no_subscriber
```

O que observar:

```text
[PUBSUB] tópico sem subscribers: mensagem descartada
[ACK] discarded_no_subscribers
```

---

### Teste 2 — Fluxo normal Pub/Sub

Objetivo:

Mostrar o fluxo básico de funcionamento: subscriber assina um tópico e publisher publica nesse tópico.

Comando:

```bash
go run ./cmd/test_normal_flow
```

O que observar:

```text
[SUBSCRIBE] cliente inscrito
[PUBSUB] mensagem publicada
[DISPATCH] mensagem enviada para subscriber
```

---

### Teste 3 — Distribuição por hash

Objetivo:

Mostrar que diferentes tópicos são roteados para diferentes brokers pela biblioteca cliente.

Comando:

```bash
go run ./cmd/test_hash_distribution
```

O que observar:

```text
[HASH] Subscribe topic='localizacao_onibus' -> broker='...'
[HASH] Publish topic='alerta_rota' -> broker='...'
```

Esse teste é importante porque demonstra o balanceamento por tópico.

---

### Teste 4 — Múltiplos subscribers no mesmo tópico

Objetivo:

Mostrar que uma mensagem publicada em um tópico é entregue para todos os subscribers inscritos naquele tópico.

Comando:

```bash
go run ./cmd/test_multiple_subscribers
```

O que observar:

```text
[SUBSCRIBER-1] recebeu
[SUBSCRIBER-2] recebeu
```

---

### Teste 5 — Unsubscribe e remoção dinâmica de tópico

Objetivo:

Mostrar que o cliente pode remover a inscrição de um tópico e que o broker remove tópicos vazios.

Comando:

```bash
go run ./cmd/test_unsubscribe
```

O que observar:

```text
[UNSUBSCRIBE] cliente removido do tópico
[TOPIC] tópico removido por falta de subscribers
```

---

### Teste 6 — Sobrecarga e fila cheia

Objetivo:

Mostrar o comportamento do broker quando um tópico fica sobrecarregado.

Comando:

```bash
go run ./cmd/test_overload
```

O que observar:

```text
[BACKPRESSURE] fila cheia no tópico
[ERROR] topic_queue_full
```

Esse teste mostra que o broker não tenta armazenar mensagens infinitamente. Quando a fila enche, ele rejeita a mensagem e informa o publicador.

---

### Teste 7 — Federação entre brokers

Objetivo:

Mostrar que os brokers tentam se conectar entre si usando o endpoint `/federation`.

Comando:

```bash
docker compose logs -f
```

O que observar:

```text
[FEDERATION] tentando conectar ao peer
[FEDERATION] peer conectado
```

A federação é uma camada complementar. Ela mostra que os brokers conseguem se comunicar, mas o balanceamento principal do projeto é feito pelo hash consistente na biblioteca cliente.

---

## 15. Comandos úteis

### Subir brokers

```bash
docker compose up --build
```

### Subir brokers em background

```bash
docker compose up -d --build
```

### Ver logs

```bash
docker compose logs -f
```

### Parar brokers

```bash
docker compose down
```

### Rodar publisher

```bash
go run ./cmd/publisher
```

### Rodar subscriber

```bash
go run ./cmd/subscriber
```

### Rodar todos os testes críticos manualmente

```bash
go run ./cmd/test_no_subscriber
go run ./cmd/test_normal_flow
go run ./cmd/test_hash_distribution
go run ./cmd/test_multiple_subscribers
go run ./cmd/test_unsubscribe
go run ./cmd/test_overload
```

---

## 16. Limitações conhecidas

Algumas limitações importantes da versão atual:

- as mensagens são mantidas apenas em memória;
- se um broker cair, mensagens na fila desse broker podem ser perdidas;
- a federação ainda não implementa tolerância a falhas completa;
- não há persistência em banco de dados;
- não há autenticação de clientes;
- o `CheckOrigin` do WebSocket deve ser restringido em ambiente real;
- o hash por tópico pode gerar gargalo se um único tópico receber tráfego demais.

Sobre o último ponto: se um tópico ficar muito sobrecarregado, ele pode virar um hot topic. A solução atual é usar fila bufferizada e backpressure. Uma evolução possível seria particionar tópicos muito carregados, por exemplo:

```text
orders#0
orders#1
orders#2
orders#3
```

Assim, um único tópico lógico poderia ser distribuído entre mais de um broker.

---

## 17. Conclusão

O NotificationHub implementa um middleware Pub/Sub distribuído com Go e WebSocket.

A principal decisão arquitetural foi usar hash consistente na biblioteca cliente para distribuir tópicos entre múltiplos brokers, mantendo transparência para publishers e subscribers.

O projeto também possui uma camada de federação entre brokers, filas bufferizadas por tópico, tratamento de mensagens sem subscribers, backpressure em caso de sobrecarga e aplicações de exemplo para demonstrar o funcionamento.

De forma resumida, o sistema mostra como processos distribuídos podem se comunicar de maneira assíncrona, desacoplada e escalável usando uma arquitetura Publish/Subscribe.
