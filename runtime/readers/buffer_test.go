package readers

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func registerAggregation(t *testing.T, buffer *SignalBuffer, cell string, strategy AggregationStrategy) {
	t.Helper()
	if err := buffer.RegisterAggregation(cell, strategy, "", ""); err != nil {
		t.Fatalf("register aggregation: %v", err)
	}
}

func mustAggregation(t *testing.T, res SignalFlushResult, cell string) SignalAggregationResult {
	t.Helper()
	agg, ok := res.Aggregations[cell]
	if !ok {
		t.Fatalf("aggregation %s missing", cell)
	}
	return agg
}

func TestSignalBufferLast(t *testing.T) {
	buffer, err := NewSignalBuffer("signal", 3)
	if err != nil {
		t.Fatalf("new signal buffer: %v", err)
	}
	registerAggregation(t, buffer, "cell", AggregationLast)

	ts1 := time.Now()
	if err := buffer.Push(ts1, 1.0, nil); err != nil {
		t.Fatalf("push sample 1: %v", err)
	}
	ts2 := ts1.Add(time.Millisecond)
	if err := buffer.Push(ts2, 2.0, nil); err != nil {
		t.Fatalf("push sample 2: %v", err)
	}
	res, err := buffer.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	agg := mustAggregation(t, res, "cell")
	if !agg.HasValue {
		t.Fatalf("expected aggregated value")
	}
	if agg.Aggregate.Count != 2 {
		t.Fatalf("expected count 2, got %d", agg.Aggregate.Count)
	}
	if agg.Aggregate.Overflow {
		t.Fatalf("unexpected overflow flag")
	}
	if got, ok := agg.Aggregate.Value.(float64); !ok || got != 2.0 {
		t.Fatalf("expected value 2.0, got %v", agg.Aggregate.Value)
	}
	if !agg.Aggregate.Timestamp.Equal(ts2) {
		t.Fatalf("expected timestamp %v, got %v", ts2, agg.Aggregate.Timestamp)
	}
}

func TestSignalBufferSum(t *testing.T) {
	buffer, err := NewSignalBuffer("signal", 4)
	if err != nil {
		t.Fatalf("new signal buffer: %v", err)
	}
	registerAggregation(t, buffer, "cell", AggregationSum)

	now := time.Now()
	for i := 1; i <= 3; i++ {
		if err := buffer.Push(now.Add(time.Duration(i)*time.Millisecond), float64(i), nil); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	res, err := buffer.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	agg := mustAggregation(t, res, "cell")
	if !agg.HasValue {
		t.Fatalf("expected aggregated value")
	}
	if agg.Aggregate.Overflow {
		t.Fatalf("unexpected overflow flag")
	}
	if got, ok := agg.Aggregate.Value.(float64); !ok || got != 6.0 {
		t.Fatalf("expected sum 6.0, got %v", agg.Aggregate.Value)
	}
}

func TestSignalBufferMean(t *testing.T) {
	buffer, err := NewSignalBuffer("signal", 3)
	if err != nil {
		t.Fatalf("new signal buffer: %v", err)
	}
	registerAggregation(t, buffer, "cell", AggregationMean)

	now := time.Now()
	samples := []float64{2, 4, 6}
	for i, sample := range samples {
		if err := buffer.Push(now.Add(time.Duration(i)*time.Millisecond), sample, nil); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	res, err := buffer.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	agg := mustAggregation(t, res, "cell")
	if !agg.HasValue {
		t.Fatalf("expected aggregated value")
	}
	if got, ok := agg.Aggregate.Value.(float64); !ok || got != 4.0 {
		t.Fatalf("expected mean 4.0, got %v", agg.Aggregate.Value)
	}
}

func TestSignalBufferOverflow(t *testing.T) {
	buffer, err := NewSignalBuffer("signal", 2)
	if err != nil {
		t.Fatalf("new signal buffer: %v", err)
	}
	registerAggregation(t, buffer, "cell", AggregationLast)

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
	res, err := buffer.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if !res.Overflow {
		t.Fatalf("expected overflow flag")
	}
	agg := mustAggregation(t, res, "cell")
	if !agg.Aggregate.Overflow {
		t.Fatalf("expected aggregation overflow")
	}
	if got, ok := agg.Aggregate.Value.(float64); !ok || got != 3.0 {
		t.Fatalf("expected last value 3.0, got %v", agg.Aggregate.Value)
	}
}

func TestSignalBufferConcurrentPush(t *testing.T) {
	const workers = 16
	const pushes = 64
	buffer, err := NewSignalBuffer("signal", workers*pushes)
	if err != nil {
		t.Fatalf("new signal buffer: %v", err)
	}
	registerAggregation(t, buffer, "cell", AggregationSum)

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
	res, err := buffer.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	agg := mustAggregation(t, res, "cell")
	if !agg.HasValue {
		t.Fatalf("expected aggregated value")
	}
	if agg.Aggregate.Overflow {
		t.Fatalf("unexpected overflow flag")
	}
	expected := 0.0
	total := workers * pushes
	for i := 1; i <= total; i++ {
		expected += float64(i)
	}
	if got, ok := agg.Aggregate.Value.(float64); !ok || got != expected {
		t.Fatalf("expected sum %.0f, got %v", expected, agg.Aggregate.Value)
	}
}

func TestSignalBufferStatusTracksMetrics(t *testing.T) {
	buffer, err := NewSignalBuffer("signal", 2)
	if err != nil {
		t.Fatalf("new signal buffer: %v", err)
	}
	registerAggregation(t, buffer, "cell", AggregationLast)

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
	res, err := buffer.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	agg := mustAggregation(t, res, "cell")
	status := buffer.Status()
	if status.Buffered != agg.Aggregate.Count {
		t.Fatalf("expected buffered %d, got %d", agg.Aggregate.Count, status.Buffered)
	}
	if status.Dropped != 1 {
		t.Fatalf("expected dropped count 1, got %d", status.Dropped)
	}
	aggStatus, ok := status.Aggregations["cell"]
	if !ok {
		t.Fatalf("missing aggregation status")
	}
	if aggStatus.LastAggregate.Count != agg.Aggregate.Count {
		t.Fatalf("expected last aggregate count %d, got %d", agg.Aggregate.Count, aggStatus.LastAggregate.Count)
	}
	if !aggStatus.LastAggregate.Overflow {
		t.Fatalf("expected overflow flag in last aggregate")
	}
	if !aggStatus.LastAggregate.Timestamp.Equal(agg.Aggregate.Timestamp) {
		t.Fatalf("expected timestamp %v, got %v", agg.Aggregate.Timestamp, aggStatus.LastAggregate.Timestamp)
	}
}

func TestSignalBufferMultipleAggregations(t *testing.T) {
	buffer, err := NewSignalBuffer("signal", 4)
	if err != nil {
		t.Fatalf("new signal buffer: %v", err)
	}
	registerAggregation(t, buffer, "sum", AggregationSum)
	registerAggregation(t, buffer, "mean", AggregationMean)
	registerAggregation(t, buffer, "queue", AggregationQueueLength)

	now := time.Now()
	values := []float64{1, 2, 3, 4}
	for i, v := range values {
		if err := buffer.Push(now.Add(time.Duration(i)*time.Millisecond), v, nil); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	res, err := buffer.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	sumAgg := mustAggregation(t, res, "sum")
	meanAgg := mustAggregation(t, res, "mean")
	queueAgg := mustAggregation(t, res, "queue")
	if got, ok := sumAgg.Aggregate.Value.(float64); !ok || got != 10 {
		t.Fatalf("expected sum 10, got %v", sumAgg.Aggregate.Value)
	}
	if got, ok := meanAgg.Aggregate.Value.(float64); !ok || got != 2.5 {
		t.Fatalf("expected mean 2.5, got %v", meanAgg.Aggregate.Value)
	}
	if got, ok := queueAgg.Aggregate.Value.(int); !ok || got != len(values) {
		t.Fatalf("expected queue length %d, got %v", len(values), queueAgg.Aggregate.Value)
	}
}

func TestSignalBufferAggregationErrorDoesNotAffectOthers(t *testing.T) {
	buffer, err := NewSignalBuffer("signal", 2)
	if err != nil {
		t.Fatalf("new signal buffer: %v", err)
	}
	registerAggregation(t, buffer, "last", AggregationLast)
	registerAggregation(t, buffer, "sum", AggregationSum)

	now := time.Now()
	if err := buffer.Push(now, "invalid", nil); err != nil {
		t.Fatalf("push: %v", err)
	}
	res, err := buffer.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	lastAgg := mustAggregation(t, res, "last")
	sumAgg := mustAggregation(t, res, "sum")
	if lastAgg.Err != nil {
		t.Fatalf("unexpected error for last aggregation: %v", lastAgg.Err)
	}
	if !sumAgg.HasValue || sumAgg.Err == nil {
		t.Fatalf("expected error for sum aggregation")
	}
}
