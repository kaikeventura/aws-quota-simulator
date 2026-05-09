package quota

import (
	"testing"
	"time"

	"github.com/aws-quota-simulator/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestTokenBucket_Allow(t *testing.T) {
	bucket := NewTokenBucket(5, 5)

	for i := 0; i < 5; i++ {
		assert.True(t, bucket.Allow(), "Should allow tokens while bucket has capacity")
	}

	assert.False(t, bucket.Allow(), "Should deny when bucket is empty")
}

func TestTokenBucket_Refill(t *testing.T) {
	bucket := NewTokenBucket(10, 10)

	for i := 0; i < 10; i++ {
		bucket.Allow()
	}

	assert.False(t, bucket.Allow(), "Bucket should be empty")

	time.Sleep(200 * time.Millisecond)

	assert.True(t, bucket.Allow(), "Bucket should have refilled")
}

func TestTokenBucket_Capacity(t *testing.T) {
	bucket := NewTokenBucket(3, 100)

	assert.Equal(t, int64(3), bucket.Tokens(), "Should start with full capacity")

	bucket.Allow()
	bucket.Allow()

	assert.Equal(t, int64(1), bucket.Tokens(), "Should have 1 token left")
}

func TestResourceLimiter_Allow(t *testing.T) {
	limiter := NewResourceLimiter(5, 5)

	for i := 0; i < 5; i++ {
		assert.True(t, limiter.Allow("resource1"))
	}

	assert.False(t, limiter.Allow("resource1"))
}

func TestResourceLimiter_DifferentResources(t *testing.T) {
	limiter := NewResourceLimiter(2, 2)

	assert.True(t, limiter.Allow("res1"))
	assert.True(t, limiter.Allow("res1"))
	assert.False(t, limiter.Allow("res1"))

	assert.True(t, limiter.Allow("res2"), "Different resource should have separate limit")
}

func TestResourceLimiter_GetOrCreate(t *testing.T) {
	limiter := NewResourceLimiter(10, 10)

	bucket1 := limiter.GetOrCreate("res", 5, 5)
	bucket2 := limiter.GetOrCreate("res", 5, 5)

	assert.Same(t, bucket1, bucket2, "Should return same bucket for same resource")

	bucket3 := limiter.GetOrCreate("res2", 3, 3)
	assert.NotSame(t, bucket1, bucket3, "Should create new bucket for different resource")
}

func TestQuotaManager_New(t *testing.T) {
	cfg := &config.Config{
		SQS: config.SQSConfig{
			FIFO: config.FIFOQueueConfig{
				TPSLimit:   300,
				BurstLimit: 300,
			},
		},
		DynamoDB: config.DynamoDBConfig{
			OnDemand: config.OnDemandConfig{
				MaxTableRPS: 40000,
			},
		},
	}

	mgr := NewManager(cfg)

	assert.NotNil(t, mgr)

	svc, ok := mgr.GetService("sqs")
	assert.True(t, ok)
	assert.NotNil(t, svc)

	svc, ok = mgr.GetService("dynamodb")
	assert.True(t, ok)
	assert.NotNil(t, svc)
}

func TestQuotaManager_RegisterService(t *testing.T) {
	cfg := &config.Config{}
	mgr := NewManager(cfg)

	mockService := &MockQuotaService{}

	mgr.RegisterService("test", mockService)

	svc, ok := mgr.GetService("test")
	assert.True(t, ok)
	assert.Equal(t, mockService, svc)
}

func TestQuotaManager_CheckRateLimit(t *testing.T) {
	cfg := &config.Config{
		Proxy: config.ProxyConfig{
			Port:        4567,
			UpstreamURL: "http://localhost:4566",
		},
		SQS: config.SQSConfig{
			FIFO: config.FIFOQueueConfig{
				TPSLimit:   300,
				BurstLimit: 300,
			},
			Standard: config.StandardQueueConfig{
				SendTPSLimit:  100000,
				BurstLimit:   100000,
			},
		},
	}
	mgr := NewManager(cfg)

	limiter := mgr.GetLimiter("sqs", "test-queue")
	assert.NotNil(t, limiter)

	allowed, msg := mgr.CheckRateLimit("sqs", "test-queue")
	assert.True(t, allowed, "Should be allowed initially: %s", msg)
	assert.Empty(t, msg)
}

func TestQuotaManager_CheckMessageSize(t *testing.T) {
	cfg := &config.Config{
		SQS: config.SQSConfig{
			MaxMessageSize: 262144,
		},
	}
	mgr := NewManager(cfg)

	tests := []struct {
		name       string
		size       int64
		shouldPass bool
	}{
		{"Valid size", 5000, true},
		{"Empty message", 0, true},
		{"Above maximum", 300000, false},
		{"At maximum", 262144, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allowed, _ := mgr.CheckMessageSize(tt.size)
			assert.Equal(t, tt.shouldPass, allowed)
		})
	}
}

func TestQuotaManager_CheckBatchSize(t *testing.T) {
	cfg := &config.Config{
		SQS: config.SQSConfig{
			MaxBatchSize: 10,
		},
	}
	mgr := NewManager(cfg)

	tests := []struct {
		name       string
		size       int
		shouldPass bool
	}{
		{"Valid size", 5, true},
		{"At max", 10, true},
		{"Above max", 11, false},
		{"Empty batch", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allowed, _ := mgr.CheckBatchSize(tt.size)
			assert.Equal(t, tt.shouldPass, allowed)
		})
	}
}

func TestQuotaManager_CheckInflightLimit(t *testing.T) {
	cfg := &config.Config{
		SQS: config.SQSConfig{
			Standard: config.StandardQueueConfig{
				MaxInflightMessages: 100,
			},
		},
	}
	mgr := NewManager(cfg)

	allowed, msg := mgr.CheckInflightLimit("test-queue", 50)
	assert.True(t, allowed, "First check should pass, got msg: %s", msg)

	allowed, msg = mgr.CheckInflightLimit("test-queue", 60)
	assert.False(t, allowed, "Second check should fail, got msg: %s", msg)
	assert.Contains(t, msg, "in-flight")
}

type MockQuotaService struct{}

func (m *MockQuotaService) CheckLimit(service, operation, resource string) error {
	return nil
}

func (m *MockQuotaService) GetRateLimit(service, resource string) (int64, int64) {
	return 100, 100
}