package remote

import (
	"fmt"
	"time"

	"github.com/goburrow/modbus"

	"github.com/timzifer/modbus_processor/internal/config"
)

// Client defines the subset of Modbus operations required by the service.
//
// Implementations can wrap TCP, RTU or mocked transports, but must honour the
// Modbus semantics of each method and be safe for concurrent access by the
// scheduler which may trigger multiple reads and writes in quick succession.
type Client interface {
	ReadCoils(address, quantity uint16) ([]byte, error)
	ReadDiscreteInputs(address, quantity uint16) ([]byte, error)
	ReadHoldingRegisters(address, quantity uint16) ([]byte, error)
	ReadInputRegisters(address, quantity uint16) ([]byte, error)
	WriteSingleCoil(address, value uint16) ([]byte, error)
	WriteSingleRegister(address, value uint16) ([]byte, error)
	Close() error
}

// ClientFactory is responsible for creating Modbus clients for remote calls.
//
// Factories should establish the underlying connection and return a client that
// is ready for immediate use. They are invoked during configuration loading, so
// expensive retries should be avoided or time bound to keep startup responsive.
type ClientFactory func(cfg config.EndpointConfig) (Client, error)

type tcpClient struct {
	handler *modbus.TCPClientHandler
	client  modbus.Client
}

// NewTCPClientFactory returns a factory that creates TCP Modbus clients.
func NewTCPClientFactory() ClientFactory {
	return func(cfg config.EndpointConfig) (Client, error) {
		if cfg.Address == "" {
			return nil, fmt.Errorf("remote address is required")
		}
		handler := modbus.NewTCPClientHandler(cfg.Address)
		handler.SlaveId = cfg.UnitID
		timeout := cfg.Timeout.Duration
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		handler.Timeout = timeout
		if err := handler.Connect(); err != nil {
			return nil, fmt.Errorf("connect remote %s: %w", cfg.Address, err)
		}
		return &tcpClient{handler: handler, client: modbus.NewClient(handler)}, nil
	}
}

func (c *tcpClient) ReadCoils(address, quantity uint16) ([]byte, error) {
	return c.client.ReadCoils(address, quantity)
}

func (c *tcpClient) ReadDiscreteInputs(address, quantity uint16) ([]byte, error) {
	return c.client.ReadDiscreteInputs(address, quantity)
}

func (c *tcpClient) ReadHoldingRegisters(address, quantity uint16) ([]byte, error) {
	return c.client.ReadHoldingRegisters(address, quantity)
}

func (c *tcpClient) ReadInputRegisters(address, quantity uint16) ([]byte, error) {
	return c.client.ReadInputRegisters(address, quantity)
}

func (c *tcpClient) WriteSingleCoil(address, value uint16) ([]byte, error) {
	return c.client.WriteSingleCoil(address, value)
}

func (c *tcpClient) WriteSingleRegister(address, value uint16) ([]byte, error) {
	return c.client.WriteSingleRegister(address, value)
}

func (c *tcpClient) Close() error {
	if c.handler != nil {
		return c.handler.Close()
	}
	return nil
}
