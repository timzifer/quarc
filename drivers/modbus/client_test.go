package modbus

import (
	"net"
	"testing"
	"time"

	"github.com/timzifer/quarc/config"
)

func TestNewTCPClientFactoryRequiresAddress(t *testing.T) {
	factory := NewTCPClientFactory()
	_, err := factory(config.EndpointConfig{})
	if err == nil {
		t.Fatal("expected error for missing address")
	}
}

func TestNewTCPClientFactoryConnectsAndConfigures(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	connected := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		close(connected)
		conn.Close()
	}()

	factory := NewTCPClientFactory()
	endpoint := config.EndpointConfig{Address: ln.Addr().String(), UnitID: 17}

	client, err := factory(endpoint)
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Fatalf("client.Close: %v", err)
		}
	})

	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("expected connection to be established")
	}

	tcp, ok := client.(*tcpClient)
	if !ok {
		t.Fatalf("expected *tcpClient, got %T", client)
	}
	if tcp.handler.SlaveId != endpoint.UnitID {
		t.Fatalf("unexpected slave id: got %d want %d", tcp.handler.SlaveId, endpoint.UnitID)
	}
	if tcp.handler.Timeout != 5*time.Second {
		t.Fatalf("unexpected timeout: got %s want %s", tcp.handler.Timeout, 5*time.Second)
	}
}

func TestNewTCPClientFactoryConnectionFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	factory := NewTCPClientFactory()
	_, err = factory(config.EndpointConfig{Address: addr})
	if err == nil {
		t.Fatal("expected connection error")
	}
}
