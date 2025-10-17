package random

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

const (
	defaultFloatMin              = 0.0
	defaultFloatMax              = 1.0
	defaultIntMin          int64 = 0
	defaultIntMax          int64 = 100
	defaultBoolProbability       = 0.5
	defaultStringLength          = 12
	defaultAlphabet              = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
)

// Settings describes the configuration accepted via driver.settings.
type Settings struct {
	Source   string                    `json:"source,omitempty"`
	Seed     *int64                    `json:"seed,omitempty"`
	Defaults SignalSettings            `json:"defaults,omitempty"`
	Signals  map[string]SignalSettings `json:"signals,omitempty"`
}

// SignalSettings customises random value generation for a single signal.
type SignalSettings struct {
	Min             *float64 `json:"min,omitempty"`
	Max             *float64 `json:"max,omitempty"`
	IntMin          *int64   `json:"int_min,omitempty"`
	IntMax          *int64   `json:"int_max,omitempty"`
	TrueProbability *float64 `json:"true_probability,omitempty"`
	StringLength    *int     `json:"string_length,omitempty"`
	Alphabet        string   `json:"alphabet,omitempty"`
	Quality         *float64 `json:"quality,omitempty"`
}

type resolvedSignalSettings struct {
	floatMin        float64
	floatMax        float64
	intMin          int64
	intMax          int64
	boolProbability float64
	stringLength    int
	alphabet        []rune
	quality         *float64
}

func parseSettings(raw json.RawMessage) (Settings, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return Settings{}, nil
	}
	var settings Settings
	if err := json.Unmarshal(raw, &settings); err != nil {
		return Settings{}, fmt.Errorf("decode random settings: %w", err)
	}
	return settings, nil
}

func (s Settings) resolve(cellID string) (resolvedSignalSettings, error) {
	resolved := resolvedSignalSettings{
		floatMin:        defaultFloatMin,
		floatMax:        defaultFloatMax,
		intMin:          defaultIntMin,
		intMax:          defaultIntMax,
		boolProbability: defaultBoolProbability,
		stringLength:    defaultStringLength,
		alphabet:        []rune(defaultAlphabet),
	}
	if err := resolved.apply(s.Defaults, "defaults"); err != nil {
		return resolvedSignalSettings{}, err
	}
	if override, ok := s.Signals[cellID]; ok {
		if err := resolved.apply(override, cellID); err != nil {
			return resolvedSignalSettings{}, err
		}
	}
	if resolved.floatMax < resolved.floatMin {
		return resolvedSignalSettings{}, fmt.Errorf("signal %s: max must be >= min", cellID)
	}
	if resolved.intMax < resolved.intMin {
		return resolvedSignalSettings{}, fmt.Errorf("signal %s: int_max must be >= int_min", cellID)
	}
	if resolved.boolProbability < 0 || resolved.boolProbability > 1 {
		return resolvedSignalSettings{}, fmt.Errorf("signal %s: true_probability must be between 0 and 1", cellID)
	}
	if resolved.stringLength <= 0 {
		return resolvedSignalSettings{}, fmt.Errorf("signal %s: string_length must be positive", cellID)
	}
	if len(resolved.alphabet) == 0 {
		return resolvedSignalSettings{}, fmt.Errorf("signal %s: alphabet must not be empty", cellID)
	}
	if math.IsNaN(resolved.floatMin) || math.IsNaN(resolved.floatMax) {
		return resolvedSignalSettings{}, fmt.Errorf("signal %s: min/max must not be NaN", cellID)
	}
	return resolved, nil
}

func (r *resolvedSignalSettings) apply(spec SignalSettings, context string) error {
	if spec.Min != nil {
		r.floatMin = *spec.Min
	}
	if spec.Max != nil {
		r.floatMax = *spec.Max
	}
	if spec.IntMin != nil {
		r.intMin = *spec.IntMin
	} else if spec.Min != nil {
		r.intMin = int64(math.Round(*spec.Min))
	}
	if spec.IntMax != nil {
		r.intMax = *spec.IntMax
	} else if spec.Max != nil {
		r.intMax = int64(math.Round(*spec.Max))
	}
	if spec.TrueProbability != nil {
		r.boolProbability = *spec.TrueProbability
	}
	if spec.StringLength != nil {
		if *spec.StringLength <= 0 {
			return fmt.Errorf("%s: string_length must be positive", context)
		}
		r.stringLength = *spec.StringLength
	}
	if strings.TrimSpace(spec.Alphabet) != "" {
		chars := []rune(spec.Alphabet)
		if len(chars) == 0 {
			return fmt.Errorf("%s: alphabet must contain printable characters", context)
		}
		r.alphabet = chars
	}
	if spec.Quality != nil {
		r.quality = cloneQuality(spec.Quality)
	}
	return nil
}

func cloneQuality(q *float64) *float64 {
	if q == nil {
		return nil
	}
	v := *q
	return &v
}
