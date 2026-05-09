package services

import (
	"testing"

	"github.com/aws-quota-simulator/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestSQSService_IsFIFOQueue(t *testing.T) {
	cfg := &config.Config{
		SQS: config.SQSConfig{
			FIFO: config.FIFOQueueConfig{
				TPSLimit:      300,
				BurstLimit:    300,
				BatchTPSLimit: 3000,
			},
		},
	}
	svc := NewSQSService(cfg)

	tests := []struct {
		name     string
		queueURL string
		expected bool
	}{
		{"FIFO queue", "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue.fifo", true},
		{"Standard queue", "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue", false},
		{"Empty URL", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := svc.IsFIFOQueue(tt.queueURL)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSQSService_IsBatchOperation(t *testing.T) {
	cfg := &config.Config{}
	svc := NewSQSService(cfg)

	tests := []struct {
		name     string
		body     string
		expected bool
	}{
		{
			"SendMessageBatch",
			`{"QueueUrl":"http://localhost:4566/queue.fifo","Entries":[{"Id":"1","MessageBody":"test"}]}`,
			false,
		},
		{
			"SendMessageBatch with multiple entries",
			`{"QueueUrl":"http://localhost:4566/queue.fifo","Entries":[{"Id":"1","MessageBody":"test1"},{"Id":"2","MessageBody":"test2"}]}`,
			true,
		},
		{
			"Single entry is not batch",
			`{"QueueUrl":"http://localhost:4566/queue","Entries":[{"Id":"1"}]}`,
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := svc.IsBatchOperation([]byte(tt.body))
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSQSService_DetectOperation(t *testing.T) {
	cfg := &config.Config{}
	svc := NewSQSService(cfg)

	tests := []struct {
		name      string
		path      string
		body      string
		expectedOp string
	}{
		{
			"SendMessage via query",
			"/123456789012/myqueue?Action=SendMessage",
			`{"QueueUrl":"http://localhost:4566/queue","MessageBody":"test"}`,
			"send",
		},
		{
			"ReceiveMessage via query",
			"/123456789012/myqueue?Action=ReceiveMessage",
			`{"QueueUrl":"http://localhost:4566/queue","MaxNumberOfMessages":10}`,
			"receive",
		},
		{
			"DeleteMessage via query",
			"/123456789012/myqueue?Action=DeleteMessage",
			`{"QueueUrl":"http://localhost:4566/queue","ReceiptHandle":"abc123"}`,
			"delete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation, _ := svc.DetectOperation(tt.path, tt.body)
			assert.Equal(t, tt.expectedOp, operation)
		})
	}
}

func TestSQSService_ShouldThrottle(t *testing.T) {
	cfg := &config.Config{
		SQS: config.SQSConfig{
			FIFO: config.FIFOQueueConfig{
				TPSLimit:      300,
				BurstLimit:    300,
				BatchTPSLimit: 3000,
			},
			Standard: config.StandardQueueConfig{
				SendTPSLimit:     100000,
				ReceiveTPSLimit:  100000,
				DeleteTPSLimit:   100000,
				BatchTPSLimit:    100000,
			},
		},
	}
	svc := NewSQSService(cfg)

	tests := []struct {
		name       string
		operation  string
		isFIFO     bool
		isBatch    bool
		shouldThrottle bool
		expectedTPS int64
	}{
		{
			"Standard queue - send throttle",
			"send",
			false,
			false,
			true,
			100000,
		},
		{
			"Standard queue - receive throttle",
			"receive",
			false,
			false,
			true,
			100000,
		},
		{
			"Standard queue - delete throttle",
			"delete",
			false,
			false,
			true,
			100000,
		},
		{
			"Standard queue - batch throttle",
			"send_batch",
			false,
			true,
			true,
			100000,
		},
		{
			"FIFO single message - throttle",
			"send",
			true,
			false,
			true,
			300,
		},
		{
			"FIFO batch - higher throttle",
			"send_batch",
			true,
			true,
			true,
			3000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shouldThrottle, tpsLimit := svc.ShouldThrottle(tt.operation, tt.isFIFO, tt.isBatch)
			assert.Equal(t, tt.shouldThrottle, shouldThrottle)
			if shouldThrottle {
				assert.Equal(t, tt.expectedTPS, tpsLimit)
			}
		})
	}
}

func TestSQSService_DetectOperationFromAction(t *testing.T) {
	cfg := &config.Config{}
	svc := NewSQSService(cfg)

	tests := []struct {
		name       string
		action     string
		body       string
		expectedOp string
	}{
		{
			"SendMessage via header",
			"SQS.SendMessage",
			`{"QueueUrl":"http://localhost:4566/queue.fifo","MessageBody":"test"}`,
			"send",
		},
		{
			"SendMessageBatch via header",
			"SQS.SendMessageBatch",
			`{"QueueUrl":"http://localhost:4566/queue.fifo","Entries":[{"Id":"1"}]}`,
			"send_batch",
		},
		{
			"ReceiveMessage via header",
			"SQS.ReceiveMessage",
			`{"QueueUrl":"http://localhost:4566/queue"}`,
			"receive",
		},
		{
			"DeleteMessage via header",
			"SQS.DeleteMessage",
			`{"QueueUrl":"http://localhost:4566/queue","ReceiptHandle":"abc"}`,
			"delete",
		},
		{
			"DeleteMessageBatch via header",
			"SQS.DeleteMessageBatch",
			`{"QueueUrl":"http://localhost:4566/queue","Entries":[{"Id":"1"}]}`,
			"delete_batch",
		},
		{
			"CreateQueue via header",
			"SQS.CreateQueue",
			`{"QueueName":"test-queue"}`,
			"create",
		},
		{
			"ListQueues via header",
			"SQS.ListQueues",
			`{}`,
			"list",
		},
		{
			"Unknown action",
			"Unknown.Action",
			`{}`,
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation, _ := svc.DetectOperationFromAction(tt.action, tt.body)
			assert.Equal(t, tt.expectedOp, operation)
		})
	}
}

func TestSQSService_GetMessageSize(t *testing.T) {
	cfg := &config.Config{}
	svc := NewSQSService(cfg)

	tests := []struct {
		name       string
		body       string
		expectedSize int64
	}{
		{
			"Small message",
			`{"MessageBody":"hello"}`,
			5,
		},
		{
			"Empty message",
			`{"MessageBody":""}`,
			0,
		},
		{
			"Invalid JSON returns body length",
			`{invalid`,
			7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := svc.GetMessageSize([]byte(tt.body))
			assert.Equal(t, tt.expectedSize, size)
		})
	}
}

func TestSQSService_GetBatchEntryCount(t *testing.T) {
	cfg := &config.Config{}
	svc := NewSQSService(cfg)

	tests := []struct {
		name       string
		body       string
		expectedCount int
	}{
		{
			"Single entry",
			`{"QueueUrl":"http://localhost:4566/queue","Entries":[{"Id":"1"}]}`,
			1,
		},
		{
			"Multiple entries",
			`{"QueueUrl":"http://localhost:4566/queue","Entries":[{"Id":"1"},{"Id":"2"},{"Id":"3"}]}`,
			3,
		},
		{
			"Empty entries",
			`{"QueueUrl":"http://localhost:4566/queue","Entries":[]}`,
			0,
		},
		{
			"Invalid JSON",
			`{invalid`,
			0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count := svc.GetBatchEntryCount([]byte(tt.body))
			assert.Equal(t, tt.expectedCount, count)
		})
	}
}