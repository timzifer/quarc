package readers

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// AggregationStrategy controls how buffered samples are collapsed during a flush.
type AggregationStrategy string

const (
	// AggregationLast keeps only the most recent value.
	AggregationLast AggregationStrategy = "last"
	// AggregationSum adds numeric values.
	AggregationSum AggregationStrategy = "sum"
	// AggregationMean averages numeric values.
	AggregationMean AggregationStrategy = "mean"
	// AggregationMin selects the minimum numeric value.
	AggregationMin AggregationStrategy = "min"
	// AggregationMax selects the maximum numeric value.
	AggregationMax AggregationStrategy = "max"
)

// ErrSignalBufferOverflow is returned when a push overwrites existing samples.
var ErrSignalBufferOverflow = errors.New("signal buffer overflow")

// ParseAggregationStrategy normalises the textual representation of an aggregation strategy.
func ParseAggregationStrategy(value string) (AggregationStrategy, error) {
	if value == "" {
		return AggregationLast, nil
	}
	switch AggregationStrategy(value) {
	case AggregationLast, AggregationSum, AggregationMean, AggregationMin, AggregationMax:
		return AggregationStrategy(value), nil
	default:
		return "", fmt.Errorf("unknown aggregation strategy %q", value)
	}
}

type signalSample struct {
	value   interface{}
	ts      time.Time
	quality *float64
}

// SignalAggregate represents the collapsed view of a buffer after flushing.
type SignalAggregate struct {
	Value     interface{}
	Timestamp time.Time
	Quality   *float64
	Count     int
	Overflow  bool
}

// SignalBuffer is a fixed-size ring buffer for signal samples.
type SignalBuffer struct {
	id       string
	strategy AggregationStrategy
	capacity int

	mu       sync.Mutex
	samples  []signalSample
	head     int
	size     int
	overflow bool
	dropped  uint64

	lastBuffered  int
	lastAggregate SignalAggregate
}

// NewSignalBuffer constructs a new signal buffer.
func NewSignalBuffer(id string, capacity int, strategy AggregationStrategy) (*SignalBuffer, error) {
	if id == "" {
		return nil, errors.New("signal buffer id must not be empty")
	}
	if capacity <= 0 {
		return nil, fmt.Errorf("signal buffer %s must have positive capacity", id)
	}
	if strategy == "" {
		strategy = AggregationLast
	}
	buf := &SignalBuffer{
		id:       id,
		strategy: strategy,
		capacity: capacity,
		samples:  make([]signalSample, capacity),
	}
	return buf, nil
}

// ID returns the cell identifier associated with the buffer.
func (b *SignalBuffer) ID() string {
	if b == nil {
		return ""
	}
	return b.id
}

// Capacity returns the maximum number of samples retained by the buffer.
func (b *SignalBuffer) Capacity() int {
	if b == nil {
		return 0
	}
	return b.capacity
}

// Strategy returns the aggregation strategy for the buffer.
func (b *SignalBuffer) Strategy() AggregationStrategy {
	if b == nil {
		return ""
	}
	return b.strategy
}

func cloneQuality(q *float64) *float64 {
	if q == nil {
		return nil
	}
	v := *q
	return &v
}

// Push stores a sample in the buffer.
func (b *SignalBuffer) Push(ts time.Time, value interface{}, quality *float64) error {
	if b == nil {
		return errors.New("signal buffer is nil")
	}
	sample := signalSample{value: value, ts: ts, quality: cloneQuality(quality)}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.size < b.capacity {
		idx := (b.head + b.size) % b.capacity
		b.samples[idx] = sample
		b.size++
		return nil
	}

	// Buffer full – overwrite the oldest sample but record the overflow.
	b.samples[b.head] = sample
	b.head = (b.head + 1) % b.capacity
	b.overflow = true
	b.dropped++
	return ErrSignalBufferOverflow
}

// Flush collapses buffered samples using the configured aggregation strategy.
func (b *SignalBuffer) Flush() (SignalAggregate, bool, error) {
	if b == nil {
		return SignalAggregate{}, false, errors.New("signal buffer is nil")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.size == 0 {
		overflow := b.overflow
		b.overflow = false
		b.lastBuffered = 0
		b.lastAggregate = SignalAggregate{Overflow: overflow}
		return SignalAggregate{Overflow: overflow}, false, nil
	}

	samples := make([]signalSample, b.size)
	for i := 0; i < b.size; i++ {
		idx := (b.head + i) % b.capacity
		samples[i] = b.samples[idx]
	}
	count := b.size
	overflow := b.overflow
	b.head = 0
	b.size = 0
	b.overflow = false
	b.lastBuffered = count

	agg, err := aggregateSamples(b.strategy, samples)
	if err != nil {
		b.lastAggregate = SignalAggregate{Count: count, Overflow: overflow}
		return SignalAggregate{}, true, err
	}
	agg.Count = count
	agg.Overflow = overflow
	b.lastAggregate = agg
	return agg, true, nil
}

// Status returns the most recently observed metrics for the buffer.
func (b *SignalBuffer) Status() SignalBufferStatus {
	if b == nil {
		return SignalBufferStatus{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return SignalBufferStatus{
		Buffered:      b.lastBuffered,
		Dropped:       b.dropped,
		LastAggregate: b.lastAggregate,
	}
}

func aggregateSamples(strategy AggregationStrategy, samples []signalSample) (SignalAggregate, error) {
	switch strategy {
	case AggregationLast:
		last := samples[len(samples)-1]
		return SignalAggregate{Value: last.value, Timestamp: last.ts, Quality: cloneQuality(last.quality)}, nil
	case AggregationSum, AggregationMean, AggregationMin, AggregationMax:
		return aggregateNumeric(strategy, samples)
	default:
		return SignalAggregate{}, fmt.Errorf("unsupported aggregation strategy %q", strategy)
	}
}

func aggregateNumeric(strategy AggregationStrategy, samples []signalSample) (SignalAggregate, error) {
	if len(samples) == 0 {
		return SignalAggregate{}, errors.New("no samples to aggregate")
	}
	values := make([]float64, len(samples))
	for i, sample := range samples {
		v, err := toFloat64(sample.value)
		if err != nil {
			return SignalAggregate{}, err
		}
		values[i] = v
	}
	last := samples[len(samples)-1]
	quality := cloneQuality(last.quality)
	switch strategy {
	case AggregationSum:
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return SignalAggregate{Value: sum, Timestamp: last.ts, Quality: quality}, nil
	case AggregationMean:
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		mean := sum / float64(len(values))
		return SignalAggregate{Value: mean, Timestamp: last.ts, Quality: quality}, nil
	case AggregationMin:
		min := values[0]
		for _, v := range values[1:] {
			if v < min {
				min = v
			}
		}
		return SignalAggregate{Value: min, Timestamp: last.ts, Quality: quality}, nil
	case AggregationMax:
		max := values[0]
		for _, v := range values[1:] {
			if v > max {
				max = v
			}
		}
		return SignalAggregate{Value: max, Timestamp: last.ts, Quality: quality}, nil
	default:
		return SignalAggregate{}, fmt.Errorf("unsupported numeric aggregation %q", strategy)
	}
}

func toFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	default:
		return 0, fmt.Errorf("value %T is not numeric", value)
	}
}

// SignalBufferStore manages SignalBuffer instances keyed by cell identifier.
type SignalBufferStore struct {
	mu      sync.RWMutex
	buffers map[string]*SignalBuffer
}

// NewSignalBufferStore creates an empty buffer registry.
func NewSignalBufferStore() *SignalBufferStore {
	return &SignalBufferStore{buffers: make(map[string]*SignalBuffer)}
}

// Configure ensures a buffer exists for the given cell using the provided settings.
func (s *SignalBufferStore) Configure(cellID string, capacity int, strategy AggregationStrategy) (*SignalBuffer, error) {
	if s == nil {
		return nil, errors.New("signal buffer store is nil")
	}
	if cellID == "" {
		return nil, errors.New("cell id must not be empty")
	}
	if capacity <= 0 {
		return nil, fmt.Errorf("cell %s buffer size must be positive", cellID)
	}
	if strategy == "" {
		strategy = AggregationLast
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.buffers[cellID]; ok {
		if existing.Capacity() != capacity {
			return nil, fmt.Errorf("cell %s buffer already configured with capacity %d", cellID, existing.Capacity())
		}
		if existing.Strategy() != strategy {
			return nil, fmt.Errorf("cell %s buffer already configured with strategy %s", cellID, existing.Strategy())
		}
		return existing, nil
	}
	buf, err := NewSignalBuffer(cellID, capacity, strategy)
	if err != nil {
		return nil, err
	}
	s.buffers[cellID] = buf
	return buf, nil
}

// Get retrieves the configured buffer for the specified cell.
func (s *SignalBufferStore) Get(cellID string) (*SignalBuffer, error) {
	if s == nil {
		return nil, errors.New("signal buffer store is nil")
	}
	s.mu.RLock()
	buf, ok := s.buffers[cellID]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no signal buffer configured for cell %s", cellID)
	}
	return buf, nil
}

// All returns the registered buffers sorted by cell identifier.
func (s *SignalBufferStore) All() []*SignalBuffer {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	buffers := make([]*SignalBuffer, 0, len(s.buffers))
	ids := make([]string, 0, len(s.buffers))
	for id := range s.buffers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		buffers = append(buffers, s.buffers[id])
	}
	s.mu.RUnlock()
	return buffers
}
