package io

import (
	"time"

	"github.com/rs/zerolog"

	"modbus_processor/internal/config"
)

type Cell interface {
	Config() config.CellConfig
	SetValue(value interface{}, ts time.Time, quality *float64) error
	MarkInvalid(ts time.Time, code, message string)
	CurrentValue() (interface{}, bool)
}

type CellStore interface {
	Get(id string) (Cell, error)
}

type ReadGroupStatus struct {
	ID           string
	Function     string
	Start        uint16
	Length       uint16
	Disabled     bool
	NextRun      time.Time
	LastRun      time.Time
	LastDuration time.Duration
	Source       config.ModuleReference
}

type Writer interface {
	ID() string
	Commit(now time.Time, logger zerolog.Logger) int
	SetDisabled(disabled bool)
	Status() WriteTargetStatus
	Close()
}

type ReadGroup interface {
	ID() string
	Due(now time.Time) bool
	Perform(now time.Time, logger zerolog.Logger) int
	SetDisabled(disabled bool)
	Status() ReadGroupStatus
	Close()
}

type WriteTargetStatus struct {
	ID           string
	Cell         string
	Function     string
	Address      uint16
	Disabled     bool
	LastWrite    time.Time
	LastAttempt  time.Time
	LastDuration time.Duration
	Source       config.ModuleReference
}

type ReaderDependencies struct {
	Cells CellStore
}

type WriterDependencies struct {
	Cells CellStore
}

type ReaderFactory func(cfg config.ReadGroupConfig, deps ReaderDependencies) (ReadGroup, error)

type WriterFactory func(cfg config.WriteTargetConfig, deps WriterDependencies) (Writer, error)
