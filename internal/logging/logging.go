package logging

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/grafana/loki-client-go/loki"
	"github.com/prometheus/common/model"
	"github.com/rs/zerolog"

	"modbus_processor/internal/config"
)

// Setup creates a zerolog logger according to the provided configuration.
func Setup(cfg config.LoggingConfig) (zerolog.Logger, func(), error) {
	level := zerolog.InfoLevel
	if cfg.Level != "" {
		parsed, err := zerolog.ParseLevel(strings.ToLower(cfg.Level))
		if err != nil {
			return zerolog.Logger{}, nil, fmt.Errorf("parse log level: %w", err)
		}
		level = parsed
	}

	var stdout io.Writer = os.Stdout
	if strings.EqualFold(cfg.Format, "text") {
		stdout = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	}

	writers := []io.Writer{stdout}
	cleanup := func() {}

	if cfg.Loki.Enabled {
		lokiWriter, closer, err := newLokiWriter(cfg.Loki)
		if err != nil {
			return zerolog.Logger{}, nil, err
		}
		writers = append(writers, lokiWriter)
		cleanup = func() {
			closer()
		}
	}

	multi := zerolog.MultiLevelWriter(writers...)
	logger := zerolog.New(multi).With().Timestamp().Logger().Level(level)
	return logger, cleanup, nil
}

func newLokiWriter(cfg config.LokiConfig) (io.Writer, func(), error) {
	if cfg.URL == "" {
		return nil, nil, fmt.Errorf("loki url is required")
	}
	lokiCfg, err := loki.NewDefaultConfig(cfg.URL)
	if err != nil {
		return nil, nil, fmt.Errorf("prepare loki config: %w", err)
	}
	client, err := loki.New(lokiCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("create loki client: %w", err)
	}

	labels := model.LabelSet{}
	for k, v := range cfg.Labels {
		labels[model.LabelName(k)] = model.LabelValue(v)
	}
	if len(labels) == 0 {
		labels["app"] = "modbus-processor"
	}

	writer := &lokiWriter{client: client, labels: labels}
	cleanup := func() {
		client.Stop()
	}
	return writer, cleanup, nil
}

type lokiWriter struct {
	client *loki.Client
	labels model.LabelSet
}

func (l *lokiWriter) Write(p []byte) (int, error) {
	entry := strings.TrimSpace(string(p))
	if entry == "" {
		return len(p), nil
	}
	err := l.client.Handle(l.labels, time.Now(), entry)
	return len(p), err
}
