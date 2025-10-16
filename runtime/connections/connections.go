package connections

import "github.com/timzifer/quarc/config"

// Handle represents a reusable transport connection that can be shared between drivers.
//
// Concrete implementations usually wrap network clients or buses and should be safe for
// concurrent use by multiple readers/writers. Handles must be closeable so the service
// can release resources on shutdown.
type Handle interface {
	Close() error
}

// Factory constructs a reusable connection handle based on the shared configuration.
type Factory func(cfg config.IOConnectionConfig) (Handle, error)

// Provider exposes read and write drivers to previously initialised connection handles.
type Provider interface {
	Connection(id string) (Handle, error)
}
