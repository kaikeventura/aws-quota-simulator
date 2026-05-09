package quota

import (
	"testing"
	"time"
)

func TestDedupCache_IsDuplicate_New(t *testing.T) {
	cache := NewDedupCache(5)
	queueURL := "http://localhost:4567/000000000000/test.fifo"
	dedupID := "unique-id-123"

	if cache.IsDuplicate(queueURL, dedupID) {
		t.Error("New dedup ID should not be duplicate")
	}
}

func TestDedupCache_IsDuplicate_AfterAdd(t *testing.T) {
	cache := NewDedupCache(5)
	queueURL := "http://localhost:4567/000000000000/test.fifo"
	dedupID := "unique-id-123"

	cache.Add(queueURL, dedupID)

	if !cache.IsDuplicate(queueURL, dedupID) {
		t.Error("Dedup ID should be duplicate after Add")
	}
}

func TestDedupCache_IsDuplicate_DifferentQueues(t *testing.T) {
	cache := NewDedupCache(5)
	queue1 := "http://localhost:4567/000000000000/queue1.fifo"
	queue2 := "http://localhost:4567/000000000000/queue2.fifo"
	dedupID := "same-dedup-id"

	cache.Add(queue1, dedupID)

	if !cache.IsDuplicate(queue1, dedupID) {
		t.Error("Dedup ID should be duplicate in queue1")
	}

	if cache.IsDuplicate(queue2, dedupID) {
		t.Error("Same dedup ID should not be duplicate in different queue")
	}
}

func TestDedupCache_IsDuplicate_DifferentIDs(t *testing.T) {
	cache := NewDedupCache(5)
	queueURL := "http://localhost:4567/000000000000/test.fifo"

	cache.Add(queueURL, "id-1")
	cache.Add(queueURL, "id-2")

	if !cache.IsDuplicate(queueURL, "id-1") {
		t.Error("id-1 should be duplicate")
	}

	if !cache.IsDuplicate(queueURL, "id-2") {
		t.Error("id-2 should be duplicate")
	}

	if cache.IsDuplicate(queueURL, "id-3") {
		t.Error("id-3 should not be duplicate")
	}
}

func TestDedupCache_TTLExpiry(t *testing.T) {
	cache := NewDedupCacheWithSeconds(1)
	queueURL := "http://localhost:4567/000000000000/test.fifo"
	dedupID := "ttl-test-id"

	cache.Add(queueURL, dedupID)

	if !cache.IsDuplicate(queueURL, dedupID) {
		t.Error("ID should be duplicate immediately after add")
	}

	time.Sleep(2 * time.Second)

	if cache.IsDuplicate(queueURL, dedupID) {
		t.Error("ID should not be duplicate after TTL expiry")
	}
}

func TestDedupCache_RemoveExpired(t *testing.T) {
	cache := NewDedupCacheWithSeconds(1)
	queueURL := "http://localhost:4567/000000000000/test.fifo"

	cache.Add(queueURL, "id-1")
	cache.Add(queueURL, "id-2")

	time.Sleep(2 * time.Second)

	cache.RemoveExpired()

	if cache.IsDuplicate(queueURL, "id-1") {
		t.Error("id-1 should have expired")
	}

	if cache.IsDuplicate(queueURL, "id-2") {
		t.Error("id-2 should have expired")
	}
}

func TestDedupCache_Clear(t *testing.T) {
	cache := NewDedupCache(5)
	queueURL := "http://localhost:4567/000000000000/test.fifo"

	cache.Add(queueURL, "id-1")
	cache.Add(queueURL, "id-2")

	cache.Clear()

	if cache.IsDuplicate(queueURL, "id-1") {
		t.Error("id-1 should not be duplicate after Clear")
	}

	if cache.Len() != 0 {
		t.Errorf("Cache length should be 0, got %d", cache.Len())
	}
}

func TestDedupCache_Len(t *testing.T) {
	cache := NewDedupCache(5)

	cache.Add("queue1", "id-1")
	cache.Add("queue1", "id-2")
	cache.Add("queue2", "id-1")

	if cache.Len() != 3 {
		t.Errorf("Cache length should be 3, got %d", cache.Len())
	}
}

func TestDedupCache_Concurrent(t *testing.T) {
	cache := NewDedupCache(5)
	queueURL := "http://localhost:4567/000000000000/test.fifo"

	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			cache.Add(queueURL, "id-"+string(rune('0'+id)))
			_ = cache.IsDuplicate(queueURL, "id-"+string(rune('0'+id)))
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestDedupCache_DefaultTTL(t *testing.T) {
	cache := NewDedupCache(0)

	cache.Add("queue", "id-1")

	if !cache.IsDuplicate("queue", "id-1") {
		t.Error("Default TTL should be 5 minutes")
	}
}