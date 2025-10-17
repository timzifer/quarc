package bundle

import "github.com/timzifer/quarc/drivers/modbus"
import "github.com/timzifer/quarc/service"

// WithModbus registers the Modbus reader and writer.
func WithModbus(driver string, factory modbus.ClientFactory) []service.Option {
	readerFactory := modbus.NewReaderFactory(factory)
	writerFactory := modbus.NewWriterFactory(factory)
	return []service.Option{
		service.WithReaderFactory(driver, readerFactory),
		service.WithWriterFactory(driver, writerFactory),
	}
}
