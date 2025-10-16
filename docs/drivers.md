# Drivers

QUARC is transport agnostic. Protocol implementations live in standalone Go modules under [`drivers/`](../drivers) and are opt-in at build time. Applications register the drivers they require when constructing a `service.Service` instance.

## Built-in bundle

For convenience the [`drivers/bundle`](../drivers/bundle) module exposes helper functions that install the bundled drivers:

```go
svc, err := service.New(cfg, logger, bundle.Options(modbus.NewTCPClientFactory())...)
```

Each endpoint in the configuration specifies the driver identifier so the runtime can select the matching factories.

## Custom drivers

Custom transports register their reader and writer factories using dedicated identifiers:

```go
svc, err := service.New(cfg, logger,
        service.WithReaderFactory("my-driver", myReaderFactory),
        service.WithWriterFactory("my-driver", myWriterFactory),
)
```

Driver modules receive the raw `driver_settings` YAML (CUE) node from the configuration, allowing protocol-specific options to be decoded without changing the core schema.
