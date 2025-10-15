package readers

import "testing"

func TestSignalBufferStoreConfigureAndRetrieve(t *testing.T) {
	store := NewSignalBufferStore()
	cfgs := []SignalBufferAggregationConfig{{Cell: "agg1", Strategy: AggregationSum}, {Cell: "agg2", Strategy: AggregationLast}}

	buf, err := store.Configure("signal", 4, cfgs)
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	if buf == nil {
		t.Fatalf("expected buffer instance")
	}
	if buf.Capacity() != 4 {
		t.Fatalf("expected capacity 4, got %d", buf.Capacity())
	}

	// Reconfigure with identical settings should be a no-op.
	if _, err := store.Configure("signal", 4, cfgs); err != nil {
		t.Fatalf("reconfigure: %v", err)
	}

	bySignal, err := store.Get("signal")
	if err != nil {
		t.Fatalf("get by signal: %v", err)
	}
	if bySignal != buf {
		t.Fatalf("expected same buffer for signal lookup")
	}

	byCell, err := store.Get("agg1")
	if err != nil {
		t.Fatalf("get by aggregation cell: %v", err)
	}
	if byCell != buf {
		t.Fatalf("expected buffer for aggregation cell lookup")
	}

	buffers := store.All()
	if len(buffers) != 1 {
		t.Fatalf("expected single buffer, got %d", len(buffers))
	}
	if buffers[0] != buf {
		t.Fatalf("expected configured buffer to be returned")
	}
}

func TestSignalBufferStoreConfigureRejectsConflicts(t *testing.T) {
	store := NewSignalBufferStore()
	if _, err := store.Configure("first", 2, []SignalBufferAggregationConfig{{Cell: "agg"}}); err != nil {
		t.Fatalf("configure first: %v", err)
	}
	if _, err := store.Configure("second", 2, []SignalBufferAggregationConfig{{Cell: "agg"}}); err == nil {
		t.Fatalf("expected conflict error for reused aggregation cell")
	}
}

func TestSignalBufferStoreConfigureValidatesInputs(t *testing.T) {
	store := NewSignalBufferStore()
	if _, err := store.Configure("", 1, []SignalBufferAggregationConfig{{Cell: "agg"}}); err == nil {
		t.Fatalf("expected error for empty signal id")
	}
	if _, err := store.Configure("signal", 0, []SignalBufferAggregationConfig{{Cell: "agg"}}); err == nil {
		t.Fatalf("expected error for non-positive capacity")
	}
	if _, err := store.Configure("signal", 1, nil); err == nil {
		t.Fatalf("expected error for missing aggregations")
	}
	if _, err := store.Configure("signal", 1, []SignalBufferAggregationConfig{{Cell: ""}}); err == nil {
		t.Fatalf("expected error for empty aggregation cell")
	}
}

func TestSignalBufferStoreGetValidatesInputs(t *testing.T) {
	store := NewSignalBufferStore()
	if _, err := store.Get(""); err == nil {
		t.Fatalf("expected error for empty cell id")
	}
	if _, err := store.Get("unknown"); err == nil {
		t.Fatalf("expected error for unknown cell")
	}
}

func TestSignalBufferStoreRejectsConflictingCapacity(t *testing.T) {
	store := NewSignalBufferStore()
	if _, err := store.Configure("signal", 2, []SignalBufferAggregationConfig{{Cell: "agg"}}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	if _, err := store.Configure("signal", 3, []SignalBufferAggregationConfig{{Cell: "agg"}}); err == nil {
		t.Fatalf("expected error when reconfiguring with different capacity")
	}
}
