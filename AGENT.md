# AWS Quota Simulator - Agent Instructions

Este arquivo contém todas as instruções e contexto necessário para trabalhar neste projeto.

---

## 1. Visão Geral do Projeto

**AWS Quota Simulator** é um proxy HTTP written em Go que simula os limites de quota da AWS real durante desenvolvimento local.

### Problema que resolve

- Floci e LocalStack emulam serviços AWS mas **não impõem quotas de throttling**
- Aplicações que funcionam localmente podem falhar em produção ao atingir limites reais da AWS
- Este serviço adiciona uma camada de verificação de quotas antes de代理 requisições para o emulador

### Stack Tecnológico

| Componente | Tecnologia |
|------------|------------|
| Linguagem | Go 1.21 |
| Proxy HTTP | `net/http/httputil.ReverseProxy` |
| Rate Limiting | Token Bucket Algorithm |
| Containerização | Docker + Docker Compose |
| Configuração | YAML + Environment Variables |
| Testes | go test + testify |

---

## 2. Arquitetura

```
┌─────────────┐     ┌─────────────────────┐     ┌──────────┐
│  AWS SDK    │ ──► │  Quota Simulator    │ ──► │  Floci   │
│  (App)      │     │   (localhost:4567) │     │ (:4566)  │
└─────────────┘     └─────────────────────┘     └──────────┘
                           │
                    ┌──────┴──────┐
                    │   Rate     │
                    │  Limiter   │
                    │ (Token     │
                    │  Bucket)   │
                    └────────────┘
```

### Fluxo de uma Requisição

1. **Receção**: App envia requisição para `http://localhost:4567`
2. **Detecção de Serviço**: Verifica Host header → identifica SQS ou DynamoDB
3. **Detecção de Operação**: Lê header `X-Amz-Target` → identifica SendMessage, PutItem, etc.
4. **Extração de Recurso**: Parse do body → QueueUrl (SQS) ou TableName (DynamoDB)
5. **Verificação de Quota**: Token Bucket por recurso (fila/tabela)
6. **Ação**:
   - Se dentro do limite → forward para Floci
   - Se excedido → retorna erro AWS-compatible (400) sem forward
7. **Resposta**: Retorna resposta do Floci ou erro de throttling

---

## 3. Estrutura do Projeto

```
aws-quota-simulator/
├── cmd/server/
│   └── main.go                    # Entry point, inicialização do servidor
│
├── internal/
│   ├── config/
│   │   └── config.go              # Carregamento de YAML + override via env vars
│   │
│   ├── errors/
│   │   └── aws_errors.go           # Geração de erros no formato AWS
│   │   └── aws_errors_test.go      # Testes unitários
│   │
│   ├── proxy/
│   │   └── handler.go              # HTTP handler com detecção e quota checking
│   │
│   └── quota/
│       ├── manager.go              # Controlador central de quotas + TokenBucket
│       ├── manager_test.go        # Testes do manager
│       └── services/
│           ├── sqs.go              # Implementação SQS (FIFO throttling)
│           ├── sqs_test.go
│           ├── dynamodb.go         # Implementação DynamoDB (throughput)
│           └── dynamodb_test.go
│
├── configs/
│   └── quotas.yaml                 # Configurações default de quotas
│
├── docker-compose.yml             # Orquestração Floci + Quota Simulator
├── Dockerfile                     # Container do Quota Simulator
├── run.sh                         # Script para build + up
├── go.mod / go.sum                # Dependências Go
├── README.md                      # Documentação para usuários
├── AGENT.md                       # Este arquivo
└── .gitignore
```

---

## 4. Componentes Principais

### 4.1 Proxy Handler (`internal/proxy/handler.go`)

Responsável por interceptar todas as requisições HTTP.

**Fluxo:**
```go
func (h *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    // 1. Detecta serviço via Host header
    service := detectService(path)
    if service == "" { service = detectServiceFromHost(host) }
    if service == "" { service = detectServiceFromQuery(query) }

    // 2. Detecta operação via X-Amz-Target header
    action := r.Header.Get("X-Amz-Target")

    // 3. Lê body da requisição
    bodyBytes, _ := io.ReadAll(r.Body)

    // 4. Verifica quota
    allowed, message := h.checkQuota(service, path, query, action, bodyBytes)

    // 5. Se não permitido, retorna erro AWS
    if !allowed {
        errors.SendThrottlingError(w, service, message)
        return
    }

    // 6. Forward para Floci
    h.reverseProxy.ServeHTTP(w, r)
}
```

**Importante:**
- Usa header `X-Amz-Target` para detectar operações (padrão AWS SDK v2)
- Body deve ser lido ANTES de fazer check de quota
- Body deve ser recriado após leitura com `io.NopCloser`

### 4.2 Quota Manager (`internal/quota/manager.go`)

Responsável pelo gerenciamento central de quotas e rate limiting.

**Estrutura principal:**
```go
type QuotaManager struct {
    services map[string]QuotaService    // Serviços registrados (SQS, DynamoDB)
    limiters map[string]*ResourceLimiter // Limiter por serviço
    cfg      *config.Config              // Configuração
}

type ResourceLimiter struct {
    rate, burst int64
    buckets     map[string]*TokenBucket  // TokenBucket por recurso (fila/tabela)
}
```

**Token Bucket:**
```go
type TokenBucket struct {
    capacity   int64     // Capacidade máxima
    tokens     int64     // Tokens disponíveis
    refillRate int64     // Taxa de refill por segundo
    lastRefill time.Time // Última vez que hizo refill
}
```

### 4.3 Serviços de Quota (`internal/quota/services/`)

Cada serviço implementa a interface `QuotaService`:

```go
type QuotaService interface {
    // Detecta operação a partir do header X-Amz-Target e body
    DetectOperationFromAction(action, body string) (operation, resource string)

    // Verifica se deve aplicar throttling e retorna o limite de TPS
    ShouldThrottle(operation string, isFIFO, isBatch bool) (shouldThrottle bool, tpsLimit int64)

    // Parsers específicos
    ParseQueueURL(body []byte) string
    IsFIFOQueue(queueURL string) bool
    IsBatchOperation(body []byte) bool

    // Configurações
    GetRateLimit(service, resource string) (rate, burst int64)
}
```

**SQS Service (sqs.go):**
- Detecta operações via header `X-Amz-Target` (ex: `SQS.SendMessage`)
- Identifica filas FIFO pelo sufixo `.fifo` na URL
- Identifica operações batch pelo número de entries
- Aplica limits: 300 TPS (single), 3000 TPS (batch)

**DynamoDB Service (dynamodb.go):**
- Detecta operações via header `X-Amz-Target` (ex: `DynamoDB.PutItem`)
- Extrai TableName do body
- Aplica limits por tabela

### 4.4 Erros AWS (`internal/errors/aws_errors.go`)

Gera erros no formato exato da AWS para que o SDK tratelhe corretamente:

```go
type AWSError struct {
    Type      string `json:"__type"`    // Ex: com.amazonaws.sqs.v#RequestThrottled
    Message   string `json:"message"`
    Retryable bool   `json:"retryable,omitempty"`
}
```

**Erros suportados:**
| Serviço | Código | Tipo |
|---------|--------|------|
| SQS | RequestThrottled | com.amazonaws.sqs.v#RequestThrottled |
| DynamoDB | ProvisionedThroughputExceededException | com.amazonaws.dynamodb.v#... |
|通用| ThrottlingException | com.amazonaws.sdk.v1#ThrottlingException |

### 4.5 Configuração (`internal/config/config.go`)

Carrega configurações de dois lugares:
1. **YAML** (`configs/quotas.yaml`) - valores default
2. **Environment Variables** - override

**Variáveis suportadas:**
```go
// Environment variable overrides
QUOTA_SQS_FIFO_TPS=300
QUOTA_SQS_FIFO_BATCH_TPS=3000
QUOTA_DYNAMODB_ONDEMAND_MAX_RPS=40000
PROXY_PORT=4567
UPSTREAM_URL=http://localhost:4566
STARTUP_DELAY=5
CONFIG_PATH=configs/quotas.yaml
```

---

## 5. Detecção de Serviço e Operação

### 5.1 Detecção de Serviço

O proxy detecta qual serviço AWS está sendo chamado:

```go
func detectService(path string) string {
    // Via path (URL)
    if strings.Contains(pathLower, "sqs") { return "sqs" }
    if strings.Contains(pathLower, "dynamodb") { return "dynamodb" }
}

func detectServiceFromHost(host string) string {
    // Via Host header
    if strings.Contains(hostLower, "sqs") { return "sqs" }
    if strings.Contains(hostLower, "localhost:4567") { return "sqs" } // Fallback
}
```

### 5.2 Detecção de Operação

**CRÍTICO**: O AWS SDK v2 usa o header `X-Amz-Target` para especificar operações:

```
X-Amz-Target: SQS.SendMessage
X-Amz-Target: SQS.SendMessageBatch
X-Amz-Target: DynamoDB.PutItem
```

```go
func (s *SQSService) DetectOperationFromAction(action, body string) (operation, resource string) {
    actionLower := strings.ToLower(action)

    if strings.Contains(actionLower, "sendmessage") {
        if strings.Contains(actionLower, "batch") { return "send_batch", "" }
        return "send", ""
    }
    if strings.Contains(actionLower, "receivemessage") { return "receive", "" }
    if strings.Contains(actionLower, "deletemessage") { return "delete", "" }
    // ...
}
```

### 5.3 Detecção de Recurso

- **SQS**: Extrai `QueueUrl` do body JSON
- **DynamoDB**: Extrai `TableName` do body JSON

```go
func (s *SQSService) ParseQueueURL(body []byte) string {
    var req struct{ QueueUrl string }
    json.Unmarshal(body, &req)
    return req.QueueUrl
}
```

---

## 6. Rate Limiting (Token Bucket)

### 6.1 Algoritmo

```
A cada request:
1. Calcula tokens refill desde o último request
2. Adiciona tokens refill ao balde
3. Se tokens > 0: decrementa e Permite
4. Se tokens = 0: Bloqueia (throttle)
```

### 6.2 Configuração por Serviço

**SQS FIFO:**
- Limit: 300 TPS (sem batch), 3000 TPS (com batch)
- Burst: Igual ao limit

**DynamoDB:**
- On-demand: 40,000 RPS por tabela
- Provisioned: RCU/WCU configuráveis

---

## 7. AWS Quotas Reais

### SQS

| Tipo | Limite | Observação |
|------|--------|------------|
| Standard Queue | ~100,000 TPS | Praticamente ilimitado |
| FIFO (single) | 300 TPS | Por ação (send/receive/delete) |
| FIFO (batch) | 3,000 TPS | Com até 10 mensagens por chamada |

### DynamoDB

| Tipo | Limite | Observação |
|------|--------|------------|
| On-demand (initial) | 4,000 WRU/s | Initial, dobra peak anterior |
| On-demand (max) | 40,000 RPS | Por tabela |
| On-demand (account) | 40,000 RPS | Total account |
| Provisioned (table) | 40,000 RCU/WCU | Por tabela |
| Provisioned (account) | 80,000 RCU/WCU | Total account |

---

## 8. Convenções de Código

### 8.1 Estrutura de Arquivos Go

- **Package name**: matching nome do diretório (`quota`, `services`, `proxy`)
- **Arquivos**: `nome.go` para código, `nome_test.go` para testes
- **Imports**: group by standard, external, internal

### 8.2 Nomenclatura

- **Variáveis**: `camelCase` (ex: `quotaManager`, `tokenBucket`)
- **Constantes**: `PascalCase` (ex: `TPSLimit`, `MaxTableRPS`)
- **Funções exportadas**: `PascalCase` (ex: `NewQuotaManager`)
- **Funções privadas**: `camelCase` (ex: `detectService`)

### 8.3 Estrutura de Tests

```go
func TestNomeDoQueEstaTestando(t *testing.T) {
    cfg := &config.Config{...}
    svc := NewService(cfg)

    tests := []struct {
        name     string
        input    string
        expected string
    }{
        {"descrição caso 1", "input1", "expected1"},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := svc.Method(tt.input)
            assert.Equal(t, tt.expected, result)
        })
    }
}
```

### 8.4 Error Handling

- Usar `fmt.Errorf` com `%w` para wrapping errors
- Retornar erros específicos do domínio
- Logar erros antes de retornar

---

## 9. Comandos Úteis

### Desenvolvimento
```bash
# Build e subir containers
./run.sh

# Ou manualmente
docker compose down && docker compose build && docker compose up -d

# Desenvolvimento local (sem Docker)
go run cmd/server/main.go
```

### Testes
```bash
# Todos os testes
go test ./...

# Com coverage
go test -cover ./...

# Testes específicos
go test -v ./internal/quota/services/... -run "TestSQS"
```

### Build
```bash
# Compilar
go build -o quota-simulator ./cmd/server/main.go

# Build Docker
docker build -t quota-simulator .
```

---

## 10. Adicionando Novo Serviço

Para adicionar suporte a um novo serviço AWS (ex: Lambda, SNS):

### Passo 1: Criar arquivo de serviço

`internal/quota/services/lambda.go`

```go
package services

type LambdaService struct {
    cfg *config.Config
}

func NewLambdaService(cfg *config.Config) *LambdaService {
    return &LambdaService{cfg: cfg}
}

func (s *LambdaService) DetectOperationFromAction(action, body string) (operation, resource string) {
    // Implementar detecção via X-Amz-Target header
    // Ex: AWSLambda.Invoke → "invoke"
}

func (s *LambdaService) ShouldThrottle(operation string) (bool, int64) {
    // Definir limits de throttle
    return true, 1000 // 1000 TPS
}
```

### Passo 2: Registrar no Manager

`internal/quota/manager.go`

```go
func NewManager(cfg *config.Config) *QuotaManager {
    // ...
    m.RegisterService("lambda", services.NewLambdaService(cfg))
    return m
}
```

### Passo 3: Adicionar detecção no Handler

`internal/proxy/handler.go`

```go
func detectServiceFromQuery(query string) string {
    // Adicionar detecção de lambda
    if strings.Contains(queryLower, "invoke") { return "lambda" }
}
```

### Passo 4: Adicionar verificação de quota

`internal/proxy/handler.go`

```go
func (h *ProxyHandler) checkLambdaQuota(...) (bool, string) {
    // Implementar verificação
}
```

### Passo 5: Criar testes

`internal/quota/services/lambda_test.go`

```go
func TestLambdaService_xxx(t *testing.T) { ... }
```

---

## 11. Troubleshooting Comum

### Problema: "context canceled"

- **Causa**: Cliente fez timeout ou desconectou
- **Solução**: Normal, não é erro do proxy

### Problema: Throttling não funciona

- **Verificar**:
  1. Está usando porta 4567 (Quota Simulator), não 4566 (Floci)
  2. Header `X-Amz-Target` está sendo enviado
  3. Logs mostram "SQS check - operation: send"
- **Debug**: `docker logs quota-simulator`

### Problema: "Upstream unavailable"

- **Causa**: Floci não está rodando
- **Solução**: `docker compose up -d floci`

---

## 12. Referências

- [AWS SQS Limits](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-quotas.html)
- [AWS DynamoDB Limits](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html)
- [AWS SDK v2 Request Flow](https://docs.aws.amazon.com/sdk-for-go/v2/api-guide/making-requests)
- [Token Bucket Algorithm](https://en.wikipedia.org/wiki/Token_bucket)