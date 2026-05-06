# AWS Quota Simulator

> Um proxy que adiciona controle de quotas da AWS real aos emuladores locais como Floci e LocalStack.

## Por que usar?

Quando você desenvolve localmente com Floci ou LocalStack, os serviços funcionam perfeitamente sem limites. Mas na AWS real existem quotas de throttling que podem quebrar sua aplicação em produção.

**Este serviço resolve isso** - ele proxy as requisições e aplica os mesmos limites de taxa (rate limits) que a AWS real:

- **SQS FIFO**: 300 TPS (send/receive/delete), 3000 TPS com batching
- **DynamoDB**: 40,000 RPS por tabela (on-demand)
- Responses de erro compatíveis com SDK da AWS

## Arquitetura

```
┌─────────────┐     ┌─────────────────────┐     ┌──────────┐
│  Sua App   │ ──► │ Quota Simulator    │ ──► │  Floci   │
│  (SDK AWS) │     │   (localhost:4567)  │     │ (:4566)  │
└─────────────┘     └─────────────────────┘     └──────────┘
                        ↓
              Verifica quotas e retorna
              erros de throttling se excedido
```

## Quick Start

### 1. Iniciar os serviços

```bash
# Clone o repositório
cd aws-quota-simulator

# Iniciar com Docker Compose
./run.sh
```

Isso vai subir:
- **Quota Simulator**: `http://localhost:4567`
- **Floci**: `http://localhost:4566`

### 2. Configurar sua aplicação

Aponte seu SDK para o Quota Simulator (porta **4567**, não 4566!):

#### Option 1: Variável de ambiente

```bash
export AWS_ENDPOINT_URL=http://localhost:4567
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1
```

#### Option 2: AWS CLI

```bash
aws configure set aws_endpoint_url http://localhost:4567
```

#### Option 3: Código (Go)

```go
sqsClient := sqs.NewFromConfig(&aws.Config{
    Region:      aws.String("us-east-1"),
    Endpoint:    aws.String("http://localhost:4567"),
    Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
})
```

#### Option 4: Código (Python/boto3)

```python
import boto3

sqs = boto3.client('sqs',
    endpoint_url='http://localhost:4567',
    region_name='us-east-1',
    aws_access_key_id='test',
    aws_secret_access_key='test'
)
```

#### Option 5: Java

```java
AmazonSQS sqs = AmazonSQSClientBuilder.standard()
    .withEndpoint("http://localhost:4567")
    .withRegion("us-east-1")
    .withCredentials(new StaticCredentialsProvider(
        new BasicAWSCredentials("test", "test")
    ))
    .build();
```

### 3. Testar

```bash
# Criar uma fila FIFO
aws --endpoint-url=http://localhost:4567 sqs create-queue \
    --queue-name=test-queue.fifo \
    --attributes='{"FifoQueue":"true"}'

# Enviar mensagem (注意: FIFO tem limite de 300 TPS!)
aws --endpoint-url=http://localhost:4567 sqs send-message \
    --queue-url=http://localhost:4567/000000000000/test-queue.fifo \
    --message-body="Hello World" \
    --message-group-id="group-1"
```

## Configuração

### Arquivo de Configuração

Edite `configs/quotas.yaml`:

```yaml
sqs:
  standard:
    rate_limit: 100000   # TPS para filas padrão
    burst_limit: 100000
  fifo:
    tps_limit: 300        # Limite por ação (send/receive/delete)
    burst_limit: 300
    batch_tps_limit: 3000 # Com batching (ate 10 msgs por chamada)

dynamodb:
  on_demand:
    initial_read_rps: 4000
    initial_write_rps: 4000
    account_max_read_rps: 40000
    account_max_write_rps: 40000
    max_table_rps: 40000
  provisioned:
    per_table_max_rcu: 40000
    per_table_max_wcu: 40000
    account_max_rcu: 80000
    account_max_wcu: 80000

proxy:
  host: "0.0.0.0"
  port: 4567
  upstream_url: "http://localhost:4566"
```

### Variáveis de Ambiente

| Variável | Descrição | Padrão |
|----------|-----------|--------|
| `QUOTA_SQS_FIFO_TPS` | TPS limite para FIFO | 300 |
| `QUOTA_SQS_FIFO_BATCH_TPS` | TPS com batching | 3000 |
| `QUOTA_DYNAMODB_ONDEMAND_MAX_RPS` | RPS max DynamoDB | 40000 |
| `PROXY_PORT` | Porta do proxy | 4567 |
| `UPSTREAM_URL` | URL do Floci | http://localhost:4566 |
| `STARTUP_DELAY` | Delay (segundos) | 0 |
| `CONFIG_PATH` | Path do arquivo yaml | configs/quotas.yaml |

Exemplo docker-compose:

```yaml
quota-simulator:
  environment:
    - QUOTA_SQS_FIFO_TPS=300
    - QUOTA_DYNAMODB_ONDEMAND_MAX_RPS=40000
```

## Simulando Throttling

### SQS FIFO - Exemplo de teste

Crie um script de teste para verificar o throttling:

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
)

func main() {
    ctx := context.Background()

    cfg, _ := config.LoadDefaultConfig(ctx,
        config.WithRegion("us-east-1"),
        config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
            func(service, region string, options ...interface{}) (aws.Endpoint, error) {
                return aws.Endpoint{URL: "http://localhost:4567"}, nil
            }),
        ),
    )

    client := sqs.NewFromConfig(cfg)

    queueURL := "http://localhost:4567/000000000000/test-queue.fifo"

    var count int
    var lastTPS time.Time
    tpsCounter := 0

    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()

    for {
        _, err := client.SendMessage(ctx, &sqs.SendMessageInput{
            QueueUrl:          aws.String(queueURL),
            MessageBody:       aws.String(fmt.Sprintf("test-%d", count)),
            MessageGroupId:    aws.String("group-1"),
        })

        if err != nil {
            fmt.Printf("Throttling detected: %v\n", err)
            time.Sleep(100 * time.Millisecond)
            continue
        }

        count++
        tpsCounter++

        select {
        case <-ticker.C:
            fmt.Printf("Count: %d, TPS: %d\n", count, tpsCounter)
            tpsCounter = 0
        default:
        }
    }
}
```

Quando exceder 300 TPS, você verá erros como:

```
Throttling detected: RequestThrottled: Rate limit exceeded for queue
```

### DynamoDB

Da mesma forma, limites são aplicados para operações de leitura/escrita. O SDK automaticamente faz retry com backoff exponencial.

## Erros Retornados

O serviço retorna erros no formato AWS:

**SQS Throttling:**
```json
HTTP/1.1 400 Bad Request
{
  "__type": "com.amazonaws.sqs.v#RequestThrottled",
  "message": "Rate exceeded for queue: test-queue.fifo",
  "retryable": true
}
```

**DynamoDB Throttling:**
```json
HTTP/1.1 400 Bad Request
{
  "__type": "com.amazonaws.dynamodb.v#ProvisionedThroughputExceededException",
  "message": "Rate exceeded for table: my-table",
  "retryable": true
}
```

O SDK da AWS trata esses erros automaticamente com retry.

## Desenvolvimento

### Executar localmente (sem Docker)

```bash
# Clone e build
go mod download
go build -o quota-simulator ./cmd/server/main.go

# Executar (precisa do Floci rodando na porta 4566)
./quota-simulator

# Ou com variáveis customizadas
QUOTA_SQS_FIFO_TPS=100 UPSTREAM_URL=http://localhost:4566 ./quota-simulator
```

### Testes

```bash
# Rodar testes unitários apenas (sem Docker)
go test -short ./...

# Rodar todos os testes (requer Docker rodando)
go test ./...

# Com verbose
go test -v ./internal/...

# Cover
go test -cover ./...

# Pular explicitamente testes de integração
SKIP_INTEGRATION=1 go test ./...
```

#### Testes de Integração com Testcontainers

O projeto inclui testes de integração que usam testcontainers para validar o comportamento de throttling:

```bash
# Rodar apenas testes de integração (requer Docker)
go test -v ./internal/quota/services/... -run "Integration"

# Ver logs dos containers durante os testes
go test -v -count=1 ./internal/quota/services/... -run "TestSQSFIFO"
```

**Casos de teste disponíveis:**

| Teste | Descrição |
|-------|-----------|
| `TestSQSFIFOThrottling_SingleMessage` | Valida throttling em SendMessage (FIFO) |
| `TestSQSFIFOThrottling_BatchMessages` | Valida throttling em SendMessageBatch |
| `TestSQSStandardQueue_NoThrottle` | Valida que standard queues não têm throttle |
| `TestSQSMultipleQueues_IndependentThrottling` | Valida throttle independente por fila |
| `TestSQSReceiveMessage_Throttling` | Testa operações de receive |
| `TestSQSQueueCreation_WithoutThrottling` | Valida que criação de filas não é throttleada |

**Configuração de quotas para testes:**

Os testes de integração usam quotas reduzidas para permitir validação rápida:

- `QUOTA_SQS_FIFO_TPS=5` (em vez de 300)
- `QUOTA_SQS_FIFO_BATCH_TPS=10` (em vez de 3000)

Isso permite testar throttling em poucos segundos ao invés de precisar atingir 300+ TPS.

### Adicionar novo serviço

1. Crie `internal/quota/services/novoservico.go`
2. Implemente a interface `QuotaService`:

```go
type QuotaService interface {
    DetectOperationFromAction(action, body string) (operation, resource string)
    CheckLimit(service, operation, resource string) error
    GetRateLimit(service, resource string) (rate, burst int64)
}
```

3. Registre em `internal/quota/manager.go`:

```go
m.RegisterService("novoservico", services.NewNovoServicoService(cfg))
```

## Troubleshooting

**Throttling não funciona:**
- Verifique se está usando a porta 4567 (Quota Simulator), não 4566 (Floci)
- Use `docker logs -f quota-simulator` para ver os logs de detecção

**Client disconnected errors:**
- São normais quando o cliente faz timeout (úsual em testes de carga)

**Erro "Upstream unavailable":**
- Verifique se o Floci está rodando: `docker ps`
- Verifique logs: `docker logs floci`

## Tech Stack

- **Go 1.21** - Linguagem
- **Token Bucket** - Rate limiting algorithm
- **httputil.ReverseProxy** - Proxy HTTP
- **Docker** - Containerização

## Licença

MIT