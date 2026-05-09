package quota

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

type DedupEntry struct {
	expiresAt time.Time
}

type DedupCache struct {
	entries map[string]map[string]*DedupEntry
	mu      sync.RWMutex
	ttl     time.Duration
}

func NewDedupCache(ttlMinutes int) *DedupCache {
	ttl := 5
	if ttlMinutes > 0 {
		ttl = ttlMinutes
	}
	return &DedupCache{
		entries: make(map[string]map[string]*DedupEntry),
		ttl:     time.Duration(ttl) * time.Minute,
	}
}

func NewDedupCacheWithSeconds(ttlSeconds int) *DedupCache {
	ttl := 300
	if ttlSeconds > 0 {
		ttl = ttlSeconds
	}
	return &DedupCache{
		entries: make(map[string]map[string]*DedupEntry),
		ttl:     time.Duration(ttl) * time.Second,
	}
}

func (c *DedupCache) IsDuplicate(queueURL, dedupID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	queueEntries, exists := c.entries[queueURL]
	if !exists {
		return false
	}

	entry, exists := queueEntries[dedupID]
	if !exists {
		return false
	}

	if time.Now().After(entry.expiresAt) {
		delete(queueEntries, dedupID)
		if len(queueEntries) == 0 {
			delete(c.entries, queueURL)
		}
		return false
	}

	return true
}

func (c *DedupCache) Add(queueURL, dedupID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.entries[queueURL]; !exists {
		c.entries[queueURL] = make(map[string]*DedupEntry)
	}

	c.entries[queueURL][dedupID] = &DedupEntry{
		expiresAt: time.Now().Add(c.ttl),
	}
}

func (c *DedupCache) RemoveExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for queueURL, entries := range c.entries {
		for dedupID, entry := range entries {
			if now.After(entry.expiresAt) {
				delete(entries, dedupID)
			}
		}
		if len(entries) == 0 {
			delete(c.entries, queueURL)
		}
	}
}

func (c *DedupCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]map[string]*DedupEntry)
}

func (c *DedupCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	count := 0
	for _, entries := range c.entries {
		count += len(entries)
	}
	return count
}

func CalculateMessageBodyHash(body []byte) string {
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}