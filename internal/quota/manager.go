package quota

import (
	"sync"
	"time"

	"github.com/aws-quota-simulator/internal/config"
	"github.com/aws-quota-simulator/internal/quota/services"
)

type QuotaService interface {
	CheckLimit(service, operation, resource string) error
	GetRateLimit(service, resource string) (rate, burst int64)
}

type TokenBucket struct {
	capacity    int64
	tokens      int64
	refillRate  int64
	lastRefill  time.Time
	mu          sync.Mutex
}

func NewTokenBucket(capacity, refillRate int64) *TokenBucket {
	return &TokenBucket{
		capacity:   capacity,
		tokens:     capacity,
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens > 0 {
		tb.tokens--
		return true
	}
	return false
}

func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()

	if elapsed > 0 {
		refill := int64(elapsed * float64(tb.refillRate))
		tb.tokens = min(tb.capacity, tb.tokens+refill)
		tb.lastRefill = now
	}
}

func (tb *TokenBucket) Tokens() int64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill()
	return tb.tokens
}

type ResourceLimiter struct {
	rate   int64
	burst  int64
	buckets map[string]*TokenBucket
	mu      sync.RWMutex
}

func NewResourceLimiter(rate, burst int64) *ResourceLimiter {
	return &ResourceLimiter{
		rate:   rate,
		burst:  burst,
		buckets: make(map[string]*TokenBucket),
	}
}

func (rl *ResourceLimiter) Allow(resource string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	bucket, exists := rl.buckets[resource]
	if !exists {
		bucket = NewTokenBucket(rl.burst, rl.rate)
		rl.buckets[resource] = bucket
	}

	return bucket.Allow()
}

func (rl *ResourceLimiter) GetOrCreate(resource string, rate, burst int64) *TokenBucket {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if bucket, exists := rl.buckets[resource]; exists {
		return bucket
	}

	bucket := NewTokenBucket(burst, rate)
	rl.buckets[resource] = bucket
	return bucket
}

type QuotaManager struct {
	services   map[string]QuotaService
	limiters   map[string]*ResourceLimiter
	limiterMu  sync.RWMutex
	cfg        *config.Config
}

func NewQuotaManager(cfg *config.Config) *QuotaManager {
	return &QuotaManager{
		services:  make(map[string]QuotaService),
		limiters:  make(map[string]*ResourceLimiter),
		cfg:       cfg,
	}
}

func NewManager(cfg *config.Config) *QuotaManager {
	m := &QuotaManager{
		services:  make(map[string]QuotaService),
		limiters:  make(map[string]*ResourceLimiter),
		cfg:       cfg,
	}

	m.RegisterService("sqs", services.NewSQSService(cfg))
	m.RegisterService("dynamodb", services.NewDynamoDBService(cfg))

	return m
}

func (qm *QuotaManager) RegisterService(name string, service QuotaService) {
	qm.services[name] = service
}

func (qm *QuotaManager) GetService(name string) (QuotaService, bool) {
	svc, ok := qm.services[name]
	return svc, ok
}

func (qm *QuotaManager) CheckLimit(service, operation, resource string) error {
	svc, ok := qm.services[service]
	if !ok {
		return nil // Unknown service, allow
	}

	return svc.CheckLimit(service, operation, resource)
}

func (qm *QuotaManager) GetLimiter(service, resource string) *ResourceLimiter {
	key := service + ":" + resource

	qm.limiterMu.RLock()
	limiter, exists := qm.limiters[key]
	qm.limiterMu.RUnlock()

	if exists {
		return limiter
	}

	qm.limiterMu.Lock()
	defer qm.limiterMu.Unlock()

	// Double-check after acquiring write lock
	if limiter, exists = qm.limiters[key]; exists {
		return limiter
	}

	// Get default limits from config
	rate, burst := int64(100), int64(100)
	switch service {
	case "sqs":
		rate = qm.cfg.SQS.FIFO.TPSLimit
		burst = qm.cfg.SQS.FIFO.BurstLimit
	case "dynamodb":
		rate = qm.cfg.DynamoDB.OnDemand.MaxTableRPS
		burst = rate * 2
	}

	limiter = NewResourceLimiter(rate, burst)
	qm.limiters[key] = limiter

	return limiter
}

func (qm *QuotaManager) CheckRateLimit(service, resource string) (bool, string) {
	limiter := qm.GetLimiter(service, resource)

	if limiter == nil {
		return true, ""
	}

	// Use resource-specific bucket
	if limiter.Allow(resource) {
		return true, ""
	}

	return false, "Rate limit exceeded for " + resource
}