package quota

import (
	"testing"
)

func TestIsFIFOQueueByName(t *testing.T) {
	tests := []struct {
		name     string
		queueName string
		expected bool
	}{
		{"FIFO queue", "test.fifo", true},
		{"Standard queue", "test", false},
		{"FIFO with path", "queue/my-queue.fifo", true},
		{"Empty name", "", false},
		{"FIFO in middle", "test.fifo.other", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsFIFOQueueByName(tt.queueName)
			if result != tt.expected {
				t.Errorf("IsFIFOQueueByName(%q) = %v, want %v", tt.queueName, result, tt.expected)
			}
		})
	}
}

func TestIsFIFOQueue(t *testing.T) {
	tests := []struct {
		name     string
		queueURL string
		expected bool
	}{
		{"FIFO URL", "http://localhost:4567/000000000000/test.fifo", true},
		{"Standard URL", "http://localhost:4567/000000000000/test", false},
		{"HTTPS FIFO", "https://sqs.us-east-1.amazonaws.com/123456789/test.fifo", true},
		{"URL without suffix", "http://localhost:4567/000000000000/myqueue", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsFIFOQueue(tt.queueURL)
			if result != tt.expected {
				t.Errorf("IsFIFOQueue(%q) = %v, want %v", tt.queueURL, result, tt.expected)
			}
		})
	}
}

func TestExtractQueueName(t *testing.T) {
	tests := []struct {
		name     string
		queueURL string
		expected string
	}{
		{"Standard URL", "http://localhost:4567/000000000000/my-queue", "my-queue"},
		{"FIFO URL", "http://localhost:4567/000000000000/test.fifo", "test.fifo"},
		{"URL with many segments", "https://sqs.us-east-1.amazonaws.com/123456789/my-account/my-queue-name", "my-queue-name"},
		{"Empty URL", "", ""},
		{"Single segment", "just-name", "just-name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractQueueName(tt.queueURL)
			if result != tt.expected {
				t.Errorf("extractQueueName(%q) = %q, want %q", tt.queueURL, result, tt.expected)
			}
		})
	}
}

func TestFIFOAttributesCache_ContentBasedDeduplication(t *testing.T) {
	cache := NewFIFOAttributesCache(5, "http://localhost:4566")

	queueURL := "http://localhost:4567/000000000000/test.fifo"

	result := cache.IsContentBasedDeduplication(queueURL)

	if result {
		t.Error("IsContentBasedDeduplication should return false when queue is not in cache and fetch fails")
	}
}

func TestFIFOAttributesCache_Invalidate(t *testing.T) {
	cache := NewFIFOAttributesCache(5, "http://localhost:4566")
	queueURL := "http://localhost:4567/000000000000/test.fifo"

	cache.Invalidate(queueURL)

	attrs, _ := cache.Get(queueURL)
	if attrs == nil {
		t.Error("Get should return default attrs after Invalidate")
	}
}

func TestFIFOAttributesCache_Clear(t *testing.T) {
	cache := NewFIFOAttributesCache(5, "http://localhost:4566")

	cache.Clear()

	if len(cache.entries) != 0 {
		t.Error("Cache should be empty after Clear")
	}
}