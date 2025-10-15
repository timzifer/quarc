package random

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	mathrand "math/rand"
	"strings"
	"time"
)

// randomSource abstracts the random number generator used by the driver.
type randomSource interface {
	Float64() (float64, error)
	Int63() (int64, error)
	ReadBytes(p []byte) error
}

// pseudoSource wraps math/rand to provide deterministic pseudo random numbers.
type pseudoSource struct {
	rng *mathrand.Rand
}

func newPseudoSource(seed *int64) *pseudoSource {
	var src mathrand.Source
	if seed != nil {
		src = mathrand.NewSource(*seed)
	} else {
		src = mathrand.NewSource(time.Now().UnixNano())
	}
	return &pseudoSource{rng: mathrand.New(src)}
}

func (s *pseudoSource) Float64() (float64, error) {
	return s.rng.Float64(), nil
}

func (s *pseudoSource) Int63() (int64, error) {
	return s.rng.Int63(), nil
}

func (s *pseudoSource) ReadBytes(p []byte) error {
	_, err := s.rng.Read(p)
	return err
}

// secureSource uses crypto/rand to provide cryptographically strong randomness.
type secureSource struct{}

func (secureSource) Float64() (float64, error) {
	v, err := secureSource{}.Int63()
	if err != nil {
		return 0, err
	}
	return float64(v) / float64(math.MaxInt64), nil
}

func (secureSource) Int63() (int64, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0, fmt.Errorf("secure source: %w", err)
	}
	// Mask the sign bit to keep the value in the positive range.
	val := binary.BigEndian.Uint64(buf[:]) & math.MaxInt64
	return int64(val), nil
}

func (secureSource) ReadBytes(p []byte) error {
	if _, err := rand.Read(p); err != nil {
		return fmt.Errorf("secure source: %w", err)
	}
	return nil
}

func newRandomSource(source string, seed *int64) (randomSource, error) {
	switch normalized := normalizeSource(source); normalized {
	case "", "pseudo", "mersenne", "math":
		return newPseudoSource(seed), nil
	case "secure", "crypto":
		return secureSource{}, nil
	default:
		return nil, fmt.Errorf("unknown random source %q", source)
	}
}

func normalizeSource(src string) string {
	normalized := strings.TrimSpace(strings.ToLower(src))
	return normalized
}

func randomFloatInRange(src randomSource, min, max float64) (float64, error) {
	if min == max {
		return min, nil
	}
	if max < min {
		return 0, fmt.Errorf("invalid float range [%f, %f]", min, max)
	}
	sample, err := src.Float64()
	if err != nil {
		return 0, err
	}
	return min + (max-min)*sample, nil
}

func randomIntInRange(src randomSource, min, max int64) (int64, error) {
	if min == max {
		return min, nil
	}
	if max < min {
		return 0, fmt.Errorf("invalid integer range [%d, %d]", min, max)
	}
	span := max - min + 1
	if span <= 0 {
		return 0, fmt.Errorf("integer range overflow for [%d, %d]", min, max)
	}
	limit := (math.MaxInt64 / span) * span
	for {
		value, err := src.Int63()
		if err != nil {
			return 0, err
		}
		if value < limit {
			return min + value%span, nil
		}
	}
}

func randomBool(src randomSource, probability float64) (bool, error) {
	if probability <= 0 {
		return false, nil
	}
	if probability >= 1 {
		return true, nil
	}
	sample, err := src.Float64()
	if err != nil {
		return false, err
	}
	return sample < probability, nil
}

func randomString(src randomSource, length int, alphabet []rune) (string, error) {
	if length <= 0 {
		return "", nil
	}
	if len(alphabet) == 0 {
		return "", fmt.Errorf("alphabet must not be empty")
	}
	builder := strings.Builder{}
	builder.Grow(length * 2)
	for i := 0; i < length; i++ {
		idx, err := randomIntInRange(src, 0, int64(len(alphabet)-1))
		if err != nil {
			return "", err
		}
		builder.WriteRune(alphabet[idx])
	}
	return builder.String(), nil
}
