package remote

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timzifer/modbus_processor/config"
)

func TestNewTCPClientFactoryRequiresAddress(t *testing.T) {
	factory := NewTCPClientFactory()
	_, err := factory(config.EndpointConfig{})
	require.Error(t, err)
}

func TestNewTCPClientFactoryConnectsAndConfigures(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
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
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("expected connection to be established")
	}

	tcp, ok := client.(*tcpClient)
	require.True(t, ok, "expected *tcpClient")
	require.Equal(t, endpoint.UnitID, tcp.handler.SlaveId)
	require.Equal(t, 5*time.Second, tcp.handler.Timeout)
}

func TestNewTCPClientFactoryConnectionFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	factory := NewTCPClientFactory()
	_, err = factory(config.EndpointConfig{Address: addr})
	require.Error(t, err)
}
