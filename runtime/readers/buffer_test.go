package readers

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSignalBufferLast(t *testing.T) {
	buffer, err := NewSignalBuffer("cell", 3, AggregationLast)
	if err != nil {
		t.Fatalf("new signal buffer: %v", err)
	}
	ts1 := time.Now()
	if err := buffer.Push(ts1, 1.0, nil); err != nil {
		t.Fatalf("push sample 1: %v", err)
	}
	ts2 := ts1.Add(time.Millisecond)
	if err := buffer.Push(ts2, 2.0, nil); err != nil {
		t.Fatalf("push sample 2: %v", err)
	}
	agg, ok, err := buffer.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if !ok {
		t.Fatalf("expected aggregated value")
	}
	if agg.Count != 2 {
		t.Fatalf("expected count 2, got %d", agg.Count)
	}
	if agg.Overflow {
		t.Fatalf("unexpected overflow flag")
	}
	if got, ok := agg.Value.(float64); !ok || got != 2.0 {
		t.Fatalf("expected value 2.0, got %v", agg.Value)
	}
	if !agg.Timestamp.Equal(ts2) {
		t.Fatalf("expected timestamp %v, got %v", ts2, agg.Timestamp)
	}
}

func TestSignalBufferSum(t *testing.T) {
	buffer, err := NewSignalBuffer("cell", 4, AggregationSum)
	if err != nil {
		t.Fatalf("new signal buffer: %v", err)
	}
	now := time.Now()
	for i := 1; i <= 3; i++ {
		if err := buffer.Push(now.Add(time.Duration(i)*time.Millisecond), float64(i), nil); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	agg, ok, err := buffer.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if !ok {
		t.Fatalf("expected aggregated value")
	}
	if agg.Overflow {
		t.Fatalf("unexpected overflow flag")
	}
	if got, ok := agg.Value.(float64); !ok || got != 6.0 {
		t.Fatalf("expected sum 6.0, got %v", agg.Value)
	}
}

func TestSignalBufferMean(t *testing.T) {
	buffer, err := NewSignalBuffer("cell", 3, AggregationMean)
	if err != nil {
		t.Fatalf("new signal buffer: %v", err)
	}
	now := time.Now()
	samples := []float64{2, 4, 6}
	for i, sample := range samples {
		if err := buffer.Push(now.Add(time.Duration(i)*time.Millisecond), sample, nil); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	agg, ok, err := buffer.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if !ok {
		t.Fatalf("expected aggregated value")
	}
	if got, ok := agg.Value.(float64); !ok || got != 4.0 {
		t.Fatalf("expected mean 4.0, got %v", agg.Value)
	}
}

func TestSignalBufferOverflow(t *testing.T) {
	buffer, err := NewSignalBuffer("cell", 2, AggregationLast)
	if err != nil {
		t.Fatalf("new signal buffer: %v", err)
	}
	now := time.Now()
	if err := buffer.Push(now, 1.0, nil); err != nil {
		t.Fatalf("push 1: %v", err)
	}
	if err := buffer.Push(now.Add(time.Millisecond), 2.0, nil); err != nil {
		t.Fatalf("push 2: %v", err)
	}
	if err := buffer.Push(now.Add(2*time.Millisecond), 3.0, nil); err == nil {
		t.Fatalf("expected overflow error")
	}
	agg, ok, err := buffer.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if !ok {
		t.Fatalf("expected aggregated value")
	}
	if !agg.Overflow {
		t.Fatalf("expected overflow flag")
	}
	if got, ok := agg.Value.(float64); !ok || got != 3.0 {
		t.Fatalf("expected last value 3.0, got %v", agg.Value)
	}
}

func TestSignalBufferConcurrentPush(t *testing.T) {
	const workers = 16
	const pushes = 64
	buffer, err := NewSignalBuffer("cell", workers*pushes, AggregationSum)
	if err != nil {
		t.Fatalf("new signal buffer: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for j := 1; j <= pushes; j++ {
				value := float64(offset*pushes + j)
				if err := buffer.Push(time.Now(), value, nil); err != nil {
					t.Errorf("push %.0f: %v", value, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	agg, ok, err := buffer.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if !ok {
		t.Fatalf("expected aggregated value")
	}
	if agg.Overflow {
		t.Fatalf("unexpected overflow flag")
	}
	expected := 0.0
	total := workers * pushes
	for i := 1; i <= total; i++ {
		expected += float64(i)
	}
	if got, ok := agg.Value.(float64); !ok || got != expected {
		t.Fatalf("expected sum %.0f, got %v", expected, agg.Value)
	}
}

func TestSignalBufferStatusTracksMetrics(t *testing.T) {
	buffer, err := NewSignalBuffer("cell", 2, AggregationLast)
	if err != nil {
		t.Fatalf("new signal buffer: %v", err)
	}
	now := time.Now()
	if err := buffer.Push(now, 1.0, nil); err != nil {
		t.Fatalf("push 1: %v", err)
	}
	ts := now.Add(time.Millisecond)
	if err := buffer.Push(ts, 2.0, nil); err != nil {
		t.Fatalf("push 2: %v", err)
	}
	if err := buffer.Push(ts.Add(time.Millisecond), 3.0, nil); !errors.Is(err, ErrSignalBufferOverflow) {
		t.Fatalf("expected overflow error, got %v", err)
	}
	agg, ok, err := buffer.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if !ok {
		t.Fatalf("expected aggregated value")
	}
	status := buffer.Status()
	if status.Buffered != agg.Count {
		t.Fatalf("expected buffered %d, got %d", agg.Count, status.Buffered)
	}
	if status.Dropped != 1 {
		t.Fatalf("expected dropped count 1, got %d", status.Dropped)
	}
	if status.LastAggregate.Count != agg.Count {
		t.Fatalf("expected last aggregate count %d, got %d", agg.Count, status.LastAggregate.Count)
	}
	if !status.LastAggregate.Overflow {
		t.Fatalf("expected overflow flag in last aggregate")
	}
	if !status.LastAggregate.Timestamp.Equal(agg.Timestamp) {
		t.Fatalf("expected timestamp %v, got %v", agg.Timestamp, status.LastAggregate.Timestamp)
	}
}
