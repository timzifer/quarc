package service

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"modbus_processor/internal/config"
	"modbus_processor/internal/remote"
)

type fakeClient struct {
	readCoilsFn     func(address, quantity uint16) ([]byte, error)
	readDiscreteFn  func(address, quantity uint16) ([]byte, error)
	readHoldingFn   func(address, quantity uint16) ([]byte, error)
	readInputFn     func(address, quantity uint16) ([]byte, error)
	writeCoilFn     func(address, value uint16) ([]byte, error)
	writeRegisterFn func(address, value uint16) ([]byte, error)
}

func (f *fakeClient) ReadCoils(address, quantity uint16) ([]byte, error) {
	if f.readCoilsFn == nil {
		return nil, fmt.Errorf("unexpected ReadCoils")
	}
	return f.readCoilsFn(address, quantity)
}

func (f *fakeClient) ReadDiscreteInputs(address, quantity uint16) ([]byte, error) {
	if f.readDiscreteFn == nil {
		return nil, fmt.Errorf("unexpected ReadDiscreteInputs")
	}
	return f.readDiscreteFn(address, quantity)
}

func (f *fakeClient) ReadHoldingRegisters(address, quantity uint16) ([]byte, error) {
	if f.readHoldingFn == nil {
		return nil, fmt.Errorf("unexpected ReadHoldingRegisters")
	}
	return f.readHoldingFn(address, quantity)
}

func (f *fakeClient) ReadInputRegisters(address, quantity uint16) ([]byte, error) {
	if f.readInputFn == nil {
		return nil, fmt.Errorf("unexpected ReadInputRegisters")
	}
	return f.readInputFn(address, quantity)
}

func (f *fakeClient) WriteSingleCoil(address, value uint16) ([]byte, error) {
	if f.writeCoilFn == nil {
		return nil, fmt.Errorf("unexpected WriteSingleCoil")
	}
	return f.writeCoilFn(address, value)
}

func (f *fakeClient) WriteSingleRegister(address, value uint16) ([]byte, error) {
	if f.writeRegisterFn == nil {
		return nil, fmt.Errorf("unexpected WriteSingleRegister")
	}
	return f.writeRegisterFn(address, value)
}

func (f *fakeClient) Close() error { return nil }

func TestIterateEvaluatesExpressions(t *testing.T) {
	cfg := &config.Config{
		LoopDuration: config.Duration{Duration: time.Millisecond},
		Coils: []config.CoilConfig{
			{BaseRegisterConfig: config.BaseRegisterConfig{Name: "coil_a", Address: 0}, InitialValue: true},
		},
		Discrete: []config.DiscreteInputConfig{
			{BaseRegisterConfig: config.BaseRegisterConfig{Name: "discrete_a", Address: 0, Expression: "coil_a"}},
		},
		Input: []config.InputRegisterConfig{
			{BaseRegisterConfig: config.BaseRegisterConfig{Name: "input_a", Address: 0, Expression: "discrete_a ? 10 : 0"}},
		},
	}

	logger := zerolog.New(io.Discard)
	srv, err := New(cfg, logger, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	if err := srv.IterateOnce(context.Background(), time.Now()); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	if snapshot := srv.DiscreteSnapshot(); len(snapshot) == 0 || !snapshot[0] {
		t.Fatalf("expected discrete input true")
	}
	if snapshot := srv.InputSnapshot(); len(snapshot) == 0 || snapshot[0] != 10 {
		t.Fatalf("expected input register 10, got %v", snapshot)
	}
}

func TestRemotePush(t *testing.T) {
	var written uint32
	client := &fakeClient{
		writeRegisterFn: func(address, value uint16) ([]byte, error) {
			atomic.StoreUint32(&written, uint32(value))
			return nil, nil
		},
	}

	factory := func(config.RemoteRegisterConfig) (remote.Client, error) {
		return client, nil
	}

	cfg := &config.Config{
		Input: []config.InputRegisterConfig{
			{BaseRegisterConfig: config.BaseRegisterConfig{
				Name:       "input_remote",
				Address:    0,
				Expression: "5",
				Remote: &config.RemoteRegisterConfig{
					Address:      "ignore:502",
					UnitID:       1,
					Type:         "holding",
					Register:     1,
					PushOnUpdate: true,
				},
			}},
		},
	}

	logger := zerolog.New(io.Discard)
	srv, err := New(cfg, logger, factory)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	if err := srv.IterateOnce(context.Background(), time.Now()); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	if got := atomic.LoadUint32(&written); got != 5 {
		t.Fatalf("expected remote write value 5, got %d", got)
	}
}

func TestRemotePull(t *testing.T) {
	client := &fakeClient{
		readCoilsFn: func(address, quantity uint16) ([]byte, error) {
			return []byte{0x01}, nil
		},
	}
	factory := func(config.RemoteRegisterConfig) (remote.Client, error) {
		return client, nil
	}

	cfg := &config.Config{
		Coils: []config.CoilConfig{
			{BaseRegisterConfig: config.BaseRegisterConfig{
				Name:    "coil_remote",
				Address: 0,
				Remote: &config.RemoteRegisterConfig{
					Address:  "ignore:502",
					UnitID:   1,
					Type:     "coil",
					Register: 0,
					Interval: config.Duration{Duration: time.Millisecond},
				},
			}},
		},
	}

	logger := zerolog.New(io.Discard)
	srv, err := New(cfg, logger, factory)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	if err := srv.IterateOnce(context.Background(), time.Now()); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	if snapshot := srv.CoilsSnapshot(); len(snapshot) == 0 || !snapshot[0] {
		t.Fatalf("expected coil to be true after remote pull")
	}
}
