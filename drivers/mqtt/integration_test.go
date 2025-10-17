package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mqttserver "github.com/mochi-co/mqtt/server"
	"github.com/mochi-co/mqtt/server/listeners"
	"github.com/rs/zerolog"

	"github.com/timzifer/quarc/config"
	"github.com/timzifer/quarc/service"
)

func TestReadGroupReceivesMessages(t *testing.T) {
	brokerURL, shutdown := startMockBroker(t)
	defer shutdown()

	settings := ReadSettings{
		Connection: ConnectionSettings{Broker: brokerURL},
		Subscriptions: []ReadSubscription{
			{
				Topic: "sensors/temperature",
				Cell:  "temperature",
				Payload: &PayloadConversion{
					Encoding:  "json",
					Path:      "value",
					ValueType: "float",
				},
			},
		},
	}

	driverSettings, err := json.Marshal(settings)
	if err != nil {
		t.Fatalf("marshal settings: %v", err)
	}

	cfg := &config.Config{
		Cycle: config.Duration{Duration: 100 * time.Millisecond},
		Logging: config.LoggingConfig{
			Level: "debug",
		},
		Telemetry: config.TelemetryConfig{},
		Policies:  config.GlobalPolicies{},
		Cells: []config.CellConfig{
			{ID: "temperature", Type: config.ValueKindFloat},
		},
		Reads: []config.ReadGroupConfig{
			{
				ID:             "rg1",
				Endpoint:       config.EndpointConfig{Driver: "mqtt"},
				DriverSettings: driverSettings,
				Signals: []config.ReadSignalConfig{
					{Cell: "temperature", Type: config.ValueKindFloat},
				},
			},
		},
	}

	svc, err := service.New(cfg, zerolog.New(io.Discard),
		service.WithReaderFactory("mqtt", NewReadFactory()),
	)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	t.Cleanup(func() {
		if cerr := svc.Close(); cerr != nil {
			t.Errorf("close service: %v", cerr)
		}
	})

	publisher := connectClient(t, brokerURL, "publisher")
	t.Cleanup(func() { publisher.Disconnect(250) })

	payload := []byte(`{"value": 21.5}`)
	token := publisher.Publish("sensors/temperature", 0, false, payload)
	if !token.WaitTimeout(5 * time.Second) {
		t.Fatal("publish timeout")
	}
	if err := token.Error(); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	waitFor(t, 5*time.Second, func() bool {
		state, err := svc.InspectCell("temperature")
		if err != nil {
			t.Fatalf("inspect cell: %v", err)
		}
		if !state.Valid {
			return false
		}
		value, ok := state.Value.(float64)
		if !ok {
			t.Fatalf("unexpected value type %T", state.Value)
		}
		return value == 21.5
	})
}

func TestWriterPublishesCellValue(t *testing.T) {
	brokerURL, shutdown := startMockBroker(t)
	defer shutdown()

	settings := WriteSettings{
		Connection: ConnectionSettings{Broker: brokerURL},
		Topic:      "actuators/output",
	}

	driverSettings, err := json.Marshal(settings)
	if err != nil {
		t.Fatalf("marshal settings: %v", err)
	}

	subClient := connectClient(t, brokerURL, "subscriber")
	t.Cleanup(func() { subClient.Disconnect(250) })

	messages := make(chan mqtt.Message, 1)
	token := subClient.Subscribe("actuators/output", 0, func(_ mqtt.Client, msg mqtt.Message) {
		select {
		case messages <- msg:
		default:
		}
	})
	if !token.WaitTimeout(5 * time.Second) {
		t.Fatal("subscribe timeout")
	}
	if err := token.Error(); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	cfg := &config.Config{
		Cycle: config.Duration{Duration: 100 * time.Millisecond},
		Logging: config.LoggingConfig{
			Level: "debug",
		},
		Telemetry: config.TelemetryConfig{},
		Policies:  config.GlobalPolicies{},
		Cells: []config.CellConfig{
			{ID: "output", Type: config.ValueKindNumber, Constant: 42},
		},
		Writes: []config.WriteTargetConfig{
			{
				ID:             "wt1",
				Cell:           "output",
				Endpoint:       config.EndpointConfig{Driver: "mqtt"},
				DriverSettings: driverSettings,
			},
		},
	}

	svc, err := service.New(cfg, zerolog.New(io.Discard),
		service.WithWriterFactory("mqtt", NewWriteFactory()),
	)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	t.Cleanup(func() {
		if cerr := svc.Close(); cerr != nil {
			t.Errorf("close service: %v", cerr)
		}
	})

	if err := svc.IterateOnce(context.Background(), time.Now()); err != nil {
		t.Fatalf("iterate once: %v", err)
	}

	select {
	case msg := <-messages:
		if string(msg.Payload()) != "42" {
			t.Fatalf("unexpected payload %q", string(msg.Payload()))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for published message")
	}
}

func startMockBroker(t *testing.T) (string, func()) {
	t.Helper()

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	server := mqttserver.NewServer(nil)
	tcp := listeners.NewTCP("test", addr)

	if err := server.AddListener(tcp, nil); err != nil {
		t.Fatalf("add listener: %v", err)
	}
	if err := server.Serve(); err != nil {
		t.Fatalf("serve: %v", err)
	}

	if err := waitForBroker(addr, 5*time.Second); err != nil {
		t.Fatalf("wait for broker: %v", err)
	}

	return "tcp://" + addr, func() {
		_ = server.Close()
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitForBroker(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("broker at %s did not start", addr)
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if cond() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("condition not satisfied within %s", timeout)
		case <-ticker.C:
		}
	}
}

func connectClient(t *testing.T, brokerURL, clientID string) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions().AddBroker(brokerURL).SetClientID(clientID)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(5 * time.Second) {
		t.Fatalf("connect timeout")
	}
	if err := token.Error(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	return client
}
