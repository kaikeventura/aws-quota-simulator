# AWS Quota Simulator

> Um proxy que adiciona controle de quotas da AWS real aos emuladores locais como Floci e LocalStack.

## Status da Implementação

| Serviço | Modo | Status | Testes de Integração |
|---------|------|--------|---------------------|
| **SQS FIFO** | - | ✅ Completo | ✅ Passando |
| **SQS Standard** | - | ✅ Completo | ✅ Passando |
| **DynamoDB** | On-Demand | ✅ Funcionando | ✅ Passando |
| **DynamoDB** | Provisioned | ❌ Em desenvolvimento | ❌ Falhando |

## Por que usar?

Quando você desenvolve localmente com Floci ou LocalStack, os serviços funcionam perfeitamente sem limites. Mas na AWS real existem quotas de throttling que podem quebrar sua aplicação em produção.

**Este serviço resolve isso** - ele proxy as requisições e aplica os mesmos limites de taxa (rate limits) que a AWS real.

---

## Compatibilidade

### ✅ Serviços Suportados

| Serviço | Status | Operações Suportadas |
|---------|--------|---------------------|
| **SQS FIFO** | ✅ Completo | SendMessage, ReceiveMessage, DeleteMessage, SendMessageBatch, DeleteMessageBatch, Deduplicação, Throttling |
| **SQS Standard** | ✅ Completo | Throttling, Message Size, Batch Size, Inflight Messages |
| **DynamoDB** | ⚠️ Parcial | On-Demand Throttling ✅ |

---

### SQS FIFO - Quotas Implementadas

#### Rate Limiting (Throttling)

| Operação | Limite Padrão (AWS) | Configurável | Variável de Ambiente |
|----------|---------------------|--------------|---------------------|
| SendMessage | 300 TPS | ✅ | `QUOTA_SQS_FIFO_TPS` |
| ReceiveMessage | 300 TPS | ✅ | `QUOTA_SQS_FIFO_RECEIVE_TPS` |
| DeleteMessage | 300 TPS | ✅ | `QUOTA_SQS_FIFO_DELETE_TPS` |
| SendMessageBatch | 3,000 TPS | ✅ | `QUOTA_SQS_FIFO_BATCH_TPS` |
| DeleteMessageBatch | 3,000 TPS | ✅ | `QUOTA_SQS_FIFO_BATCH_TPS` |

#### Limites de Mensagem (comuns a FIFO e Standard)

| Limite | Valor Padrão | Descrição |
|--------|--------------|------------|
| Message Size Max | 256 KB | Aplica a ambas filas |
| Batch Size Max | 10 msgs/call | Aplica a ambas filas |

#### Deduplicação de Mensagens

| Cenário | Comportamento | Status |
|---------|---------------|--------|
| `MessageDeduplicationId` fornecido | Verifica cache, rejeita duplicados | ✅ |
| `ContentBasedDeduplication=true` | Calcula SHA-256 do MessageBody | ✅ |
| Sem ambos | Não faz deduplicação | ✅ |
| TTL do cache | 5 minutos (padrão AWS) | ✅ |

**Erro retornado em duplicação:**
```json
{
  "__type": "com.amazonaws.sqs#DuplicateMessage",
  "message": "The message with specified message deduplication ID has already been received."
}
```

### SQS Standard - Quotas Implementadas

#### Rate Limiting (Throttling)

| Operação | Limite Padrão (Configurável) | Variável de Ambiente |
|----------|------------------------------|---------------------|
| SendMessage | 100,000 TPS | `QUOTA_SQS_STANDARD_SEND_TPS` |
| ReceiveMessage | 100,000 TPS | `QUOTA_SQS_STANDARD_RECEIVE_TPS` |
| DeleteMessage | 100,000 TPS | `QUOTA_SQS_STANDARD_DELETE_TPS` |
| SendMessageBatch | 100,000 TPS | `QUOTA_SQS_STANDARD_BATCH_TPS` |
| DeleteMessageBatch | 100,000 TPS | `QUOTA_SQS_STANDARD_BATCH_TPS` |

#### Limites de Mensagem (FIFO e Standard)

| Limite | Valor Padrão | Aplica a | Variável de Ambiente |
|--------|--------------|----------|---------------------|
| Message Size Max | 256 KB | Ambos | `QUOTA_SQS_MAX_MESSAGE_SIZE` |
| Batch Size Max | 10 msgs/call | Ambos | `QUOTA_SQS_MAX_BATCH_SIZE` |
| Inflight Messages | 120,000/fila | Standard | `QUOTA_SQS_STANDARD_MAX_INFLIGHT` |

**Erro retornado quando excede tamanho máximo:**
```json
{
  "__type": "com.amazonaws.sqs.v#RequestThrottled",
  "message": "Message size X exceeds maximum allowed size Y"
}
```

**Erro retornado quando excede batch size:**
```json
{
  "__type": "com.amazonaws.sqs.v#RequestThrottled",
  "message": "Batch contains X entries, maximum allowed is 10"
}
```

### DynamoDB - Quotas Implementadas

#### On-Demand Mode (PAY_PER_REQUEST)

| Operação | Limite Padrão (AWS) | Configurável | Variável de Ambiente |
|----------|---------------------|--------------|---------------------|
| Read (GetItem, Query, Scan) | 40,000 RRU/tabela | ✅ | `QUOTA_DYNAMODB_ONDEMAND_MAX_READ_RPS` |
| Write (PutItem, DeleteItem, UpdateItem) | 40,000 WRU/tabela | ✅ | `QUOTA_DYNAMODB_ONDEMAND_MAX_WRITE_RPS` |
| Initial Read Allocation | 2,000 RRU | ✅ | `QUOTA_DYNAMODB_ONDEMAND_INITIAL_READ_RPS` |
| Initial Write Allocation | 2,000 WRU | ✅ | `QUOTA_DYNAMODB_ONDEMAND_INITIAL_WRITE_RPS` |

#### Provisioned Mode (PROVISIONED)

| Operação | Limite Padrão (AWS) | Configurável | Variável de Ambiente |
|----------|---------------------|--------------|---------------------|
| Per-table Read (RCU) | 40,000 RCU | ✅ | `QUOTA_DYNAMODB_PROVISIONED_PER_TABLE_RCU` |
| Per-table Write (WCU) | 40,000 WCU | ✅ | `QUOTA_DYNAMODB_PROVISIONED_PER_TABLE_WCU` |
| Account-level Read | 80,000 RCU | ✅ | `QUOTA_DYNAMODB_PROVISIONED_ACCOUNT_MAX_RCU` |
| Account-level Write | 80,000 WCU | ✅ | `QUOTA_DYNAMODB_PROVISIONED_ACCOUNT_MAX_WCU` |

#### Cálculo de RCU/WCU

| Operação | Consumo |
|----------|---------|
| GetItem (4KB) | 0.5 RCU (strongly consistent) |
| GetItem (4KB) | 0.25 RCU (eventually consistent) |
| PutItem (1KB) | 1 WCU |
| DeleteItem (1KB) | 1 WCU |
| UpdateItem (1KB) | 1 WCU |
| Query (per 4KB) | 0.5 RCU |
| Scan (per 4KB) | 0.5 RCU |

**Billing Mode Detection:**
- O simulador detecta automaticamente `PROVISIONED` vs `PAY_PER_REQUEST` pelo CreateTable
- Armazena o billing mode em memória por tabela

### Limitações Conhecidas

#### DynamoDB - Em Desenvolvimento

| Funcionalidade | Status | Observação |
|----------------|--------|------------|
| **On-Demand Throttling** | ✅ Funcionando | PutItem, GetItem, DeleteItem, UpdateItem, Query, Scan |
| **Batch Operations Throttling** | ⚠️ Parcial | Detecção implementada, throttling pode variar |
| **Provisioned Mode Throttling** | ❌ Não implementado | Tabelas não são encontradas no cache |
| **Account-level Limits** | ❌ Não implementado | Necessário para provisioned mode |
| **GSI/LSI Limits** | ❌ Não implementado | Secondary indexes não suportados |
| **ReturnConsumedCapacity** | ❌ Não implementado | Resposta de capacidade não retornada |

#### Testes de Integração

```bash
# Run only On-Demand tests (these are passing):
go test ./internal/quota/services/... -run "TestOnDemandTableThrottling" -v

# Run all DynamoDB tests:
go test ./internal/quota/services/... -run "TestDynamoDB.*Throttling" -v
```

---

**Nota:** O throttling On-Demand está funcionando corretamente e os testes de integração passam. O modo Provisionado precisa de implementação adicional do cache de tabelas.

## Arquitetura

```
┌─────────────┐     ┌─────────────────────┐     ┌──────────┐
│  Sua App   │ ──► │ Quota Simulator    │ ──► │  Floci   │
│  (SDK AWS) │     │   (localhost:4567)  │     │ (:4566)  │
└─────────────┘     └─────────────────────┘     └──────────┘
                         ↓
              ┌────────────────────┐
              │   Rate Limiter     │
              │  (Token Bucket)    │
              ├────────────────────┤
              │  Dedup Cache       │
              │  (TTL 5 min)      │
              └────────────────────┘
```

---

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

# Enviar mensagem (Cuidado: FIFO tem limite de 300 TPS!)
aws --endpoint-url=http://localhost:4567 sqs send-message \
    --queue-url=http://localhost:4567/000000000000/test-queue.fifo \
    --message-body="Hello World" \
    --message-group-id="group-1"
```

---

## Configuração

### Arquivo de Configuração

Edite `configs/quotas.yaml`:

```yaml
sqs:
  standard:
    rate_limit: 100000           # Valor legacy (não usado)
    burst_limit: 100000         # Valor legacy (não usado)
    send_tps_limit: 100000      # TPS para SendMessage
    receive_tps_limit: 100000   # TPS para ReceiveMessage
    delete_tps_limit: 100000     # TPS para DeleteMessage
    batch_tps_limit: 100000     # TPS para SendMessageBatch/DeleteMessageBatch
    max_inflight_messages: 120000  # Máximo de mensagens in-flight por fila (Standard)
  fifo:
    tps_limit: 300        # Limite por ação (send/receive/delete)
    receive_tps_limit: 300
    delete_tps_limit: 300
    burst_limit: 300
    batch_tps_limit: 3000 # Com batching (ate 10 msgs por chamada)
  max_message_size: 262144    # 256 KB - Aplica a FIFO e Standard
  max_batch_size: 10          # Máximo de mensagens por batch - Aplica a FIFO e Standard
  dedup:
    ttl_minutes: 5                    # TTL do cache de deduplicação
    attributes_cache_ttl_minutes: 5   # Cache de atributos da fila

dynamodb:
  on_demand:
    initial_read_rps: 2000
    initial_write_rps: 2000
    max_table_read_rps: 40000
    max_table_write_rps: 40000
  provisioned:
    per_table_max_rcu: 40000
    per_table_max_wcu: 40000
    account_max_rcu: 80000
    account_max_wcu: 80000
    min_capacity: 1
  default:
    table_max_rcu: 10000
    table_max_wcu: 10000

proxy:
  host: "0.0.0.0"
  port: 4567
  upstream_url: "http://localhost:4566"
```

### Variáveis de Ambiente

#### SQS FIFO
| Variável | Descrição | Padrão |
|----------|-----------|--------|
| `QUOTA_SQS_FIFO_TPS` | TPS limite para SendMessage | 300 |
| `QUOTA_SQS_FIFO_RECEIVE_TPS` | TPS limite para ReceiveMessage | 300 |
| `QUOTA_SQS_FIFO_DELETE_TPS` | TPS limite para DeleteMessage | 300 |
| `QUOTA_SQS_FIFO_BATCH_TPS` | TPS com batching | 3000 |

#### SQS Standard
| Variável | Descrição | Padrão |
|----------|-----------|--------|
| `QUOTA_SQS_STANDARD_SEND_TPS` | TPS limite para SendMessage | 100000 |
| `QUOTA_SQS_STANDARD_RECEIVE_TPS` | TPS limite para ReceiveMessage | 100000 |
| `QUOTA_SQS_STANDARD_DELETE_TPS` | TPS limite para DeleteMessage | 100000 |
| `QUOTA_SQS_STANDARD_BATCH_TPS` | TPS com batching | 100000 |
| `QUOTA_SQS_STANDARD_MAX_INFLIGHT` | Máximo de mensagens in-flight (Standard) | 120000 |

#### SQS Comum (FIFO e Standard)
| Variável | Descrição | Padrão |
|----------|-----------|--------|
| `QUOTA_SQS_MAX_MESSAGE_SIZE` | Tamanho máximo da mensagem (bytes) | 262144 (256KB) |
| `QUOTA_SQS_MAX_BATCH_SIZE` | Máximo de mensagens por batch | 10 |

#### DynamoDB On-Demand
| Variável | Descrição | Padrão |
|----------|-----------|--------|
| `QUOTA_DYNAMODB_ONDEMAND_MAX_READ_RPS` | Read RPS max por tabela (RRU) | 40000 |
| `QUOTA_DYNAMODB_ONDEMAND_MAX_WRITE_RPS` | Write RPS max por tabela (WRU) | 40000 |
| `QUOTA_DYNAMODB_ONDEMAND_INITIAL_READ_RPS` | Initial read allocation | 2000 |
| `QUOTA_DYNAMODB_ONDEMAND_INITIAL_WRITE_RPS` | Initial write allocation | 2000 |

#### DynamoDB Provisioned
| Variável | Descrição | Padrão |
|----------|-----------|--------|
| `QUOTA_DYNAMODB_PROVISIONED_PER_TABLE_RCU` | Max RCU por tabela | 40000 |
| `QUOTA_DYNAMODB_PROVISIONED_PER_TABLE_WCU` | Max WCU por tabela | 40000 |
| `QUOTA_DYNAMODB_PROVISIONED_ACCOUNT_MAX_RCU` | Max RCU agregada por account | 80000 |
| `QUOTA_DYNAMODB_PROVISIONED_ACCOUNT_MAX_WCU` | Max WCU agregada por account | 80000 |

#### Geral
| Variável | Descrição | Padrão |
|----------|-----------|--------|
| `SQS_DEDUP_TTL_MINUTES` | TTL do cache de deduplicação | 5 |
| `PROXY_PORT` | Porta do proxy | 4567 |
| `UPSTREAM_URL` | URL do Floci | http://localhost:4566 |
| `STARTUP_DELAY` | Delay (segundos) | 0 |
| `CONFIG_PATH` | Path do arquivo yaml | configs/quotas.yaml |

Exemplo docker-compose:

```yaml
quota-simulator:
  environment:
    - QUOTA_SQS_FIFO_TPS=300
    - SQS_DEDUP_TTL_MINUTES=5
    - QUOTA_DYNAMODB_ONDEMAND_MAX_READ_RPS=40000
    - QUOTA_DYNAMODB_ONDEMAND_MAX_WRITE_RPS=40000
    - QUOTA_DYNAMODB_PROVISIONED_PER_TABLE_RCU=100
    - QUOTA_DYNAMODB_PROVISIONED_PER_TABLE_WCU=50
```

---

## Simulando Throttling

### SQS FIFO - Exemplo de teste

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

### DynamoDB - Exemplo de teste

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

    client := dynamodb.NewFromConfig(cfg)
    tableName := "test-table"

    // Criar tabela On-Demand
    _, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
        TableName: aws.String(tableName),
        KeySchema: []types.KeySchemaElement{
            {AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
        },
        AttributeDefinitions: []types.AttributeDefinition{
            {AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
        },
        BillingMode: types.BillingModePayPerRequest,
    })
    if err != nil {
        fmt.Printf("Error creating table: %v\n", err)
        return
    }

    time.Sleep(2 * time.Second)

    var count int
    ticker := time.NewTicker(1 * time.Second)

    for {
        _, err := client.PutItem(ctx, &dynamodb.PutItemInput{
            TableName: aws.String(tableName),
            Item: map[string]types.AttributeValue{
                "id":   &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", count)},
                "data": &types.AttributeValueMemberS{Value: "test"},
            },
        })

        if err != nil {
            fmt.Printf("Throttling detected: %v\n", err)
            time.Sleep(100 * time.Millisecond)
            continue
        }

        count++

        select {
        case <-ticker.C:
            fmt.Printf("Items written: %d\n", count)
        default:
        }
    }
}
```

Quando exceder o limite On-Demand (40,000 WRU padrão), você verá:

```
Throttling detected: ProvisionedThroughputExceededException: On-demand write throughput exceeded for table: test-table
```

---

## Simulando Deduplicação

### Exemplo com MessageDeduplicationId explícito

```go
// Primeira mensagem - será aceita
_, err := client.SendMessage(ctx, &sqs.SendMessageInput{
    QueueUrl:               aws.String(queueURL),
    MessageBody:            aws.String("Important message"),
    MessageGroupId:         aws.String("group-1"),
    MessageDeduplicationId: aws.String("my-unique-id-123"),
})

// Segunda mensagem com mesmo ID - será REJEITADA
_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
    QueueUrl:               aws.String(queueURL),
    MessageBody:            aws.String("Important message"),
    MessageGroupId:         aws.String("group-1"),
    MessageDeduplicationId: aws.String("my-unique-id-123"),
})
// Error: DuplicateMessage
```

### Exemplo com ContentBasedDeduplication

Quando a fila tem `ContentBasedDeduplication=true`, o mesmo body é automaticamente rejeitado:

```python
# Primeira mensagem - aceita
sqs.send_message(
    QueueUrl=queue_url,
    MessageBody='Same content',
    MessageGroupId='group-1'
)

# Segunda mensagem com mesmo body - REJEITADA (erro DuplicateMessage)
sqs.send_message(
    QueueUrl=queue_url,
    MessageBody='Same content',
    MessageGroupId='group-1'
)
```

---

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

**SQS DuplicateMessage:**
```json
HTTP/1.1 400 Bad Request
{
  "__type": "com.amazonaws.sqs#DuplicateMessage",
  "message": "The message with specified message deduplication ID has already been received. Queue URL: http://localhost:4567/000000000000/test.fifo",
  "retryable": false
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

---

## Desenvolvimento

### Executar localmente (sem Docker)

```bash
# Clone e build
go mod download
go build -o quota-simulator ./cmd/server/main.go

# Executar (precisa do Floci rodando na porta 4566)
./quota-simulator

# Ou com variáveis customizadas
QUOTA_SQS_FIFO_TPS=100 SQS_DEDUP_TTL_MINUTES=10 ./quota-simulator
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

O projeto inclui testes de integração que usam testcontainers para validar o comportamento de throttling e deduplicação:

```bash
# Rodar todos os testes de integração
go test -v ./internal/quota/services/...

# Rodar apenas testes de deduplicação
go test -v ./internal/quota/services/... -run "Dedup"

# Rodar apenas testes de throttling
go test -v ./internal/quota/services/... -run "Throttl"
```

**Casos de teste disponíveis - SQS:**

| Teste | Descrição |
|-------|-----------|
| `TestStandardThrottling` | Valida throttling em Standard queues |
| `TestFIFOThrottling` | Valida throttling em SendMessage (FIFO) |
| `TestFIFOReceiveThrottling` | Valida throttling em ReceiveMessage (FIFO) |
| `TestFIFODeleteThrottling` | Valida throttling em DeleteMessageBatch (FIFO) |
| `TestFIFODedupWithMessageDeduplicationId` | Valida deduplicação com ID explícito |
| `TestFIFODedupWithContentBasedDeduplication` | Valida deduplicação baseada em hash do body |
| `TestFIFONoDedupWithoutContentBasedDeduplication` | Valida que sem config não há dedup |
| `TestFIFODedupDifferentGroups` | Valida que mesmo ID em grupos diferentes é permitido |

**Casos de teste disponíveis - DynamoDB:**

| Teste | Descrição |
|-------|-----------|
| `TestOnDemandTableThrottling` | Valida throttling em tabelas On-Demand (write) |
| `TestProvisionedTableThrottling` | Valida throttling em tabelas Provisioned (write) |
| `TestProvisionedReadThrottling` | Valida throttling de leitura em tabelas Provisioned |
| `TestOnDemandQueryThrottling` | Valida throttling em Scan/Query (On-Demand) |

**Configuração de quotas para testes:**

Os testes de integração usam quotas reduzidas para permitir validação rápida:

- `QUOTA_SQS_FIFO_TPS=5` (em vez de 300)
- `QUOTA_SQS_FIFO_RECEIVE_TPS=500`
- `QUOTA_SQS_FIFO_BATCH_TPS=5` (em vez de 3000)
- `QUOTA_SQS_STANDARD_SEND_TPS=10` (para validar throttling rápido)
- `QUOTA_DYNAMODB_ONDEMAND_MAX_READ_RPS=50` (On-Demand)
- `QUOTA_DYNAMODB_ONDEMAND_MAX_WRITE_RPS=50` (On-Demand)
- `QUOTA_DYNAMODB_PROVISIONED_PER_TABLE_RCU=10` (Provisioned)
- `QUOTA_DYNAMODB_PROVISIONED_PER_TABLE_WCU=10` (Provisioned)

Isso permite testar throttling em poucos segundos ao invés de precisar atingir as quotas reais.

---

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

---

## Troubleshooting

**Throttling não funciona:**
- Verifique se está usando a porta 4567 (Quota Simulator), não 4566 (Floci)
- Use `docker logs -f quota-simulator` para ver os logs de detecção

**Client disconnected errors:**
- São normais quando o cliente faz timeout (úsual em testes de carga)

**Erro "Upstream unavailable":**
- Verifique se o Floci está rodando: `docker ps`
- Verifique logs: `docker logs floci`

**Deduplicação não funciona:**
- Verifique se a fila é FIFO (.fifo no nome)
- Verifique se `MessageDeduplicationId` está sendo enviado
- Ou se a fila tem `ContentBasedDeduplication=true`

---

## Tech Stack

- **Go 1.21** - Linguagem
- **Token Bucket** - Rate limiting algorithm
- **httputil.ReverseProxy** - Proxy HTTP
- **Docker** - Containerização
- **Testcontainers** - Testes de integração

## Licença

MIT