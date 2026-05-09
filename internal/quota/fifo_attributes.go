package quota

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"
)

type FIFOAttributes struct {
	ContentBasedDeduplication bool
	LastFetched               time.Time
}

type FIFOAttributesCache struct {
	entries map[string]*FIFOAttributes
	mu      sync.RWMutex
	ttl     time.Duration
	upstreamURL string
}

func NewFIFOAttributesCache(ttlMinutes int, upstreamURL string) *FIFOAttributesCache {
	if ttlMinutes <= 0 {
		ttlMinutes = 5
	}
	return &FIFOAttributesCache{
		entries: make(map[string]*FIFOAttributes),
		ttl:     time.Duration(ttlMinutes) * time.Minute,
		upstreamURL: strings.TrimSuffix(upstreamURL, "/"),
	}
}

func (c *FIFOAttributesCache) Get(queueURL string) (*FIFOAttributes, error) {
	c.mu.RLock()
	attrs, exists := c.entries[queueURL]
	c.mu.RUnlock()

	if exists && !c.isExpired(attrs) {
		return attrs, nil
	}

	return c.fetchAndCache(queueURL)
}

func (c *FIFOAttributesCache) fetchAndCache(queueURL string) (*FIFOAttributes, error) {
	attrs, err := c.fetchFromUpstream(queueURL)
	if err != nil {
		c.mu.Lock()
		defer c.mu.Unlock()

		if existing, exists := c.entries[queueURL]; exists {
			return existing, nil
		}
		return &FIFOAttributes{
			ContentBasedDeduplication: false,
			LastFetched:               time.Now(),
		}, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[queueURL] = attrs

	return attrs, nil
}

func (c *FIFOAttributesCache) fetchFromUpstream(queueURL string) (*FIFOAttributes, error) {
	queueName := extractQueueName(queueURL)
	if queueName == "" {
		return &FIFOAttributes{
			ContentBasedDeduplication: false,
			LastFetched:               time.Now(),
		}, nil
	}

	path := c.upstreamURL + "/_aws/sqs/GetQueueAttributes"
	body := map[string]interface{}{
		"QueueUrl":       queueURL,
		"AttributeNames": []string{"ContentBasedDeduplication", "FifoQueue"},
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", path, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return &FIFOAttributes{
			ContentBasedDeduplication: false,
			LastFetched:               time.Now(),
		}, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return &FIFOAttributes{
			ContentBasedDeduplication: false,
			LastFetched:               time.Now(),
		}, nil
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return &FIFOAttributes{
			ContentBasedDeduplication: false,
			LastFetched:               time.Now(),
		}, nil
	}

	attrs, ok := result["Attributes"].(map[string]interface{})
	if !ok {
		return &FIFOAttributes{
			ContentBasedDeduplication: false,
			LastFetched:               time.Now(),
		}, nil
	}

	contentBased := false
	if attr, exists := attrs["ContentBasedDeduplication"]; exists {
		contentBased = attr == "true"
	}

	return &FIFOAttributes{
		ContentBasedDeduplication: contentBased,
		LastFetched:               time.Now(),
	}, nil
}

func (c *FIFOAttributesCache) isExpired(attrs *FIFOAttributes) bool {
	return time.Since(attrs.LastFetched) > c.ttl
}

func (c *FIFOAttributesCache) IsContentBasedDeduplication(queueURL string) bool {
	attrs, err := c.Get(queueURL)
	if err != nil {
		return false
	}
	return attrs.ContentBasedDeduplication
}

func (c *FIFOAttributesCache) Invalidate(queueURL string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, queueURL)
}

func (c *FIFOAttributesCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]*FIFOAttributes)
}

func extractQueueName(queueURL string) string {
	parts := strings.Split(queueURL, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}

func IsFIFOQueueByName(queueName string) bool {
	return strings.HasSuffix(queueName, ".fifo")
}

func IsFIFOQueue(queueURL string) bool {
	return strings.HasSuffix(queueURL, ".fifo")
}