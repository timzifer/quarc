package service

import (
	"context"
	"errors"
	"sync"
	"time"
)

type controlMode string

const (
	controlModeRun   controlMode = "run"
	controlModePause controlMode = "pause"
)

type controlStatus struct {
	Mode        controlMode   `json:"mode"`
	Interval    time.Duration `json:"interval"`
	IntervalMS  int64         `json:"interval_ms"`
	IntervalStr string        `json:"interval_text"`
}

type cycleController struct {
	mu       sync.RWMutex
	mode     controlMode
	interval time.Duration
	notify   chan struct{}
	step     chan struct{}
}

func newCycleController(interval time.Duration) *cycleController {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	c := &cycleController{
		mode:     controlModeRun,
		interval: interval,
		notify:   make(chan struct{}, 1),
		step:     make(chan struct{}, 1),
	}
	return c
}

func (c *cycleController) Wait(ctx context.Context) (time.Time, error) {
	for {
		c.mu.RLock()
		mode := c.mode
		interval := c.interval
		c.mu.RUnlock()

		switch mode {
		case controlModeRun:
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return time.Time{}, ctx.Err()
			case <-timer.C:
				return time.Now(), nil
			case <-c.notify:
				if !timer.Stop() {
					<-timer.C
				}
				continue
			}
		case controlModePause:
			select {
			case <-ctx.Done():
				return time.Time{}, ctx.Err()
			case <-c.step:
				return time.Now(), nil
			case <-c.notify:
				continue
			}
		default:
			return time.Time{}, errors.New("unknown control mode")
		}
	}
}

func (c *cycleController) SetMode(mode controlMode) {
	c.mu.Lock()
	if c.mode == mode {
		c.mu.Unlock()
		return
	}
	c.mode = mode
	c.mu.Unlock()
	c.signal()
}

func (c *cycleController) Step() {
	c.mu.Lock()
	c.mode = controlModePause
	c.mu.Unlock()
	select {
	case c.step <- struct{}{}:
	default:
	}
}

func (c *cycleController) SetInterval(d time.Duration) {
	if d <= 0 {
		d = time.Millisecond
	}
	c.mu.Lock()
	if c.interval == d {
		c.mu.Unlock()
		return
	}
	c.interval = d
	c.mu.Unlock()
	c.signal()
}

func (c *cycleController) Status() controlStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return controlStatus{
		Mode:        c.mode,
		Interval:    c.interval,
		IntervalMS:  int64(c.interval / time.Millisecond),
		IntervalStr: c.interval.String(),
	}
}

func (c *cycleController) signal() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}
