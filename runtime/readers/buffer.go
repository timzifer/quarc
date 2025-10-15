package readers

import (
	"errors"
	"fmt"
	"sort"
	"strings"
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
	// AggregationCount counts the number of samples.
	AggregationCount AggregationStrategy = "count"
	// AggregationQueueLength reports the queue occupancy during the flush.
	AggregationQueueLength AggregationStrategy = "queue_length"
)

// ErrSignalBufferOverflow is returned when a push overwrites existing samples.
var ErrSignalBufferOverflow = errors.New("signal buffer overflow")

// ParseAggregationStrategy normalises the textual representation of an aggregation strategy.
func ParseAggregationStrategy(value string) (AggregationStrategy, error) {
	if value == "" {
		return AggregationLast, nil
	}
	switch AggregationStrategy(value) {
	case AggregationLast, AggregationSum, AggregationMean, AggregationMin, AggregationMax, AggregationCount, AggregationQueueLength:
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

// SignalAggregationResult describes the outcome of aggregating buffered samples for a specific target.
type SignalAggregationResult struct {
	Aggregate   SignalAggregate
	Strategy    AggregationStrategy
	QualityCell string
	OnOverflow  string
	Err         error
	HasValue    bool
}

// SignalFlushResult groups the results for all aggregations produced by a flush.
type SignalFlushResult struct {
	Aggregations map[string]SignalAggregationResult
	Buffered     int
	Dropped      uint64
	Overflow     bool
}

// SignalAggregationStatus captures the most recent aggregation metrics for diagnostics.
type SignalAggregationStatus struct {
	Aggregator    AggregationStrategy
	QualityCell   string
	OnOverflow    string
	LastAggregate SignalAggregate
	Error         string
}

type signalAggregator struct {
	cellID      string
	strategy    AggregationStrategy
	qualityCell string
	onOverflow  string

	lastAggregate SignalAggregate
	lastError     error
}

// SignalBuffer is a fixed-size ring buffer for signal samples.
type SignalBuffer struct {
	id       string
	capacity int

	mu       sync.Mutex
	samples  []signalSample
	head     int
	size     int
	overflow bool
	dropped  uint64

	lastBuffered int
	lastOverflow bool

	aggregators map[string]*signalAggregator
	order       []string
}

// NewSignalBuffer constructs a new signal buffer.
func NewSignalBuffer(id string, capacity int) (*SignalBuffer, error) {
	if id == "" {
		return nil, errors.New("signal buffer id must not be empty")
	}
	if capacity <= 0 {
		return nil, fmt.Errorf("signal buffer %s must have positive capacity", id)
	}
	buf := &SignalBuffer{
		id:          id,
		capacity:    capacity,
		samples:     make([]signalSample, capacity),
		aggregators: make(map[string]*signalAggregator),
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

func cloneQuality(q *float64) *float64 {
	if q == nil {
		return nil
	}
	v := *q
	return &v
}

// RegisterAggregation configures an aggregation target for the buffer.
func (b *SignalBuffer) RegisterAggregation(cellID string, strategy AggregationStrategy, qualityCell, onOverflow string) error {
	if b == nil {
		return errors.New("signal buffer is nil")
	}
	cellID = strings.TrimSpace(cellID)
	if cellID == "" {
		return errors.New("aggregation cell id must not be empty")
	}
	if strategy == "" {
		strategy = AggregationLast
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.aggregators == nil {
		b.aggregators = make(map[string]*signalAggregator)
	}
	if existing, ok := b.aggregators[cellID]; ok {
		if existing.strategy != strategy || existing.qualityCell != qualityCell || existing.onOverflow != onOverflow {
			return fmt.Errorf("aggregation for cell %s already registered with different settings", cellID)
		}
		return nil
	}
	agg := &signalAggregator{cellID: cellID, strategy: strategy, qualityCell: strings.TrimSpace(qualityCell), onOverflow: strings.TrimSpace(onOverflow)}
	b.aggregators[cellID] = agg
	b.order = append(b.order, cellID)
	return nil
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
func (b *SignalBuffer) Flush() (SignalFlushResult, error) {
	if b == nil {
		return SignalFlushResult{}, errors.New("signal buffer is nil")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.aggregators) == 0 {
		return SignalFlushResult{}, fmt.Errorf("signal buffer %s has no aggregations configured", b.id)
	}

	count := b.size
	overflow := b.overflow
	dropped := b.dropped
	samples := make([]signalSample, b.size)
	for i := 0; i < b.size; i++ {
		idx := (b.head + i) % b.capacity
		samples[i] = b.samples[idx]
	}
	b.head = 0
	b.size = 0
	b.overflow = false
	b.lastBuffered = count
	b.lastOverflow = overflow

	results := make(map[string]SignalAggregationResult, len(b.aggregators))
	if count == 0 {
		for _, cellID := range b.order {
			agg := b.aggregators[cellID]
			aggregate := SignalAggregate{Overflow: overflow}
			agg.lastAggregate = aggregate
			agg.lastError = nil
			results[cellID] = SignalAggregationResult{
				Aggregate:   aggregate,
				Strategy:    agg.strategy,
				QualityCell: agg.qualityCell,
				OnOverflow:  agg.onOverflow,
				HasValue:    false,
			}
		}
		return SignalFlushResult{Aggregations: results, Buffered: count, Dropped: dropped, Overflow: overflow}, nil
	}

	for _, cellID := range b.order {
		agg := b.aggregators[cellID]
		strategy := agg.strategy
		if strategy == "" {
			strategy = AggregationLast
		}
		aggregate, err := aggregateSamples(strategy, samples)
		if err != nil {
			last := samples[len(samples)-1]
			agg.lastAggregate = SignalAggregate{Timestamp: last.ts, Quality: cloneQuality(last.quality), Count: count, Overflow: overflow}
			agg.lastError = err
			results[cellID] = SignalAggregationResult{
				Aggregate:   agg.lastAggregate,
				Strategy:    strategy,
				QualityCell: agg.qualityCell,
				OnOverflow:  agg.onOverflow,
				Err:         err,
				HasValue:    true,
			}
			continue
		}
		aggregate.Count = count
		aggregate.Overflow = overflow
		agg.lastAggregate = aggregate
		agg.lastError = nil
		results[cellID] = SignalAggregationResult{
			Aggregate:   aggregate,
			Strategy:    strategy,
			QualityCell: agg.qualityCell,
			OnOverflow:  agg.onOverflow,
			HasValue:    true,
		}
	}

	return SignalFlushResult{Aggregations: results, Buffered: count, Dropped: dropped, Overflow: overflow}, nil
}

// Status returns the most recently observed metrics for the buffer.
func (b *SignalBuffer) Status() SignalBufferStatus {
	if b == nil {
		return SignalBufferStatus{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	aggs := make(map[string]SignalAggregationStatus, len(b.aggregators))
	for _, cellID := range b.order {
		agg := b.aggregators[cellID]
		status := SignalAggregationStatus{
			Aggregator:    agg.strategy,
			QualityCell:   agg.qualityCell,
			OnOverflow:    agg.onOverflow,
			LastAggregate: agg.lastAggregate,
		}
		if agg.lastError != nil {
			status.Error = agg.lastError.Error()
		}
		aggs[cellID] = status
	}
	return SignalBufferStatus{
		Buffered:     b.lastBuffered,
		Dropped:      b.dropped,
		Overflow:     b.lastOverflow,
		Aggregations: aggs,
	}
}

func aggregateSamples(strategy AggregationStrategy, samples []signalSample) (SignalAggregate, error) {
	switch strategy {
	case AggregationLast:
		last := samples[len(samples)-1]
		return SignalAggregate{Value: last.value, Timestamp: last.ts, Quality: cloneQuality(last.quality)}, nil
	case AggregationSum, AggregationMean, AggregationMin, AggregationMax:
		return aggregateNumeric(strategy, samples)
	case AggregationCount:
		last := samples[len(samples)-1]
		return SignalAggregate{Value: float64(len(samples)), Timestamp: last.ts, Quality: cloneQuality(last.quality)}, nil
	case AggregationQueueLength:
		last := samples[len(samples)-1]
		return SignalAggregate{Value: len(samples), Timestamp: last.ts, Quality: cloneQuality(last.quality)}, nil
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

// SignalBufferAggregationConfig describes an aggregation registration within the store.
type SignalBufferAggregationConfig struct {
	Cell        string
	Strategy    AggregationStrategy
	QualityCell string
	OnOverflow  string
}

// SignalBufferStore manages SignalBuffer instances and their aggregations.
type SignalBufferStore struct {
	mu      sync.RWMutex
	buffers map[string]*SignalBuffer
	byCell  map[string]*SignalBuffer
}

// NewSignalBufferStore creates an empty buffer registry.
func NewSignalBufferStore() *SignalBufferStore {
	return &SignalBufferStore{buffers: make(map[string]*SignalBuffer), byCell: make(map[string]*SignalBuffer)}
}

// Configure ensures a buffer exists for the given signal using the provided settings and aggregations.
func (s *SignalBufferStore) Configure(signalID string, capacity int, aggregations []SignalBufferAggregationConfig) (*SignalBuffer, error) {
	if s == nil {
		return nil, errors.New("signal buffer store is nil")
	}
	signalID = strings.TrimSpace(signalID)
	if signalID == "" {
		return nil, errors.New("signal id must not be empty")
	}
	if capacity <= 0 {
		return nil, fmt.Errorf("signal %s buffer size must be positive", signalID)
	}
	if len(aggregations) == 0 {
		return nil, fmt.Errorf("signal %s buffer requires at least one aggregation", signalID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	buf, ok := s.buffers[signalID]
	if ok {
		if buf.Capacity() != capacity {
			return nil, fmt.Errorf("signal %s buffer already configured with capacity %d", signalID, buf.Capacity())
		}
	} else {
		var err error
		buf, err = NewSignalBuffer(signalID, capacity)
		if err != nil {
			return nil, err
		}
		s.buffers[signalID] = buf
	}

	for _, aggCfg := range aggregations {
		cellID := strings.TrimSpace(aggCfg.Cell)
		if cellID == "" {
			return nil, fmt.Errorf("signal %s aggregation missing cell", signalID)
		}
		if existing, ok := s.byCell[cellID]; ok && existing != buf {
			return nil, fmt.Errorf("aggregation cell %s already registered", cellID)
		}
		if err := buf.RegisterAggregation(cellID, aggCfg.Strategy, aggCfg.QualityCell, aggCfg.OnOverflow); err != nil {
			return nil, err
		}
		s.byCell[cellID] = buf
	}
	return buf, nil
}

// Get retrieves the configured buffer for the specified aggregation or signal cell.
func (s *SignalBufferStore) Get(cellID string) (*SignalBuffer, error) {
	if s == nil {
		return nil, errors.New("signal buffer store is nil")
	}
	cellID = strings.TrimSpace(cellID)
	if cellID == "" {
		return nil, errors.New("cell id must not be empty")
	}
	s.mu.RLock()
	buf, ok := s.byCell[cellID]
	if !ok {
		buf = s.buffers[cellID]
	}
	s.mu.RUnlock()
	if buf == nil {
		return nil, fmt.Errorf("no signal buffer configured for cell %s", cellID)
	}
	return buf, nil
}

// All returns the registered buffers sorted by identifier.
func (s *SignalBufferStore) All() []*SignalBuffer {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	ids := make([]string, 0, len(s.buffers))
	for id := range s.buffers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	buffers := make([]*SignalBuffer, 0, len(ids))
	for _, id := range ids {
		buffers = append(buffers, s.buffers[id])
	}
	s.mu.RUnlock()
	return buffers
}
