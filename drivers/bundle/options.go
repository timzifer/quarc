package bundle

import (
	"github.com/timzifer/modbus_processor/drivers/canstream"
	modbus "github.com/timzifer/modbus_processor/drivers/modbus"
	"github.com/timzifer/modbus_processor/service"
)

const (
	modbusDriver = "modbus"
	canDriver    = "can"
)

// Options returns service options that register the bundled drivers.
func Options(factory modbus.ClientFactory) []service.Option {
	readerFactory := modbus.NewReaderFactory(factory)
	writerFactory := modbus.NewWriterFactory(factory)
	canReader := canstream.NewReaderFactory()
	return []service.Option{
		service.WithReaderFactory(modbusDriver, readerFactory),
		service.WithWriterFactory(modbusDriver, writerFactory),
		service.WithReaderFactory(canDriver, canReader),
	}
}

// WithModbus registers only the Modbus reader and writer.
func WithModbus(factory modbus.ClientFactory) []service.Option {
	readerFactory := modbus.NewReaderFactory(factory)
	writerFactory := modbus.NewWriterFactory(factory)
	return []service.Option{
		service.WithReaderFactory(modbusDriver, readerFactory),
		service.WithWriterFactory(modbusDriver, writerFactory),
	}
}

// WithCAN registers only the CAN stream reader.
func WithCAN() service.Option {
	return service.WithReaderFactory(canDriver, canstream.NewReaderFactory())
}
