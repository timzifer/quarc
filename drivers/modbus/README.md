# quarc_driver_modbus

A Modbus/TCP driver for the [Quarc](https://quarc.dev) industrial automation runtime. The package ships factories for reader and writer groups, configuration helpers, and a default Quarc module overlay so that Modbus-specific options can be described directly in CUE or JSON-based deployment manifests.

## Features

- **Pluggable client factory** – swap out the default TCP transport to support serial bridges, TLS terminators, or connection pools.
- **Read groups with rich decoding** – interpret coils, discrete inputs, and holding/input registers as booleans, integers, floats, or decimals with scaling, signedness, and endianness control.
- **State-aware write targets** – automatically apply deadbands, rate limiting, scaling, and signed or unsigned conversion before publishing a value back to a Modbus device.
- **Runtime overlays** – ships a `config.RegisterOverlayString` integration so Modbus fields are available in Quarc's module schema without additional plumbing.
- **Thread-safe lifecycle management** – internal caching and reuse of Modbus clients with graceful shutdown handling for both readers and writers.

## Installation

```bash
go get github.com/timzifer/quarc_driver_modbus
```

The module targets Go 1.25+. Ensure the Quarc runtime and its configuration packages are available in your project (see `go.mod`).

## Quick start

The driver exposes factories that plug into Quarc's runtime registries.

```go
package main

import (
    "context"
    "time"

    modbusdriver "github.com/timzifer/quarc_driver_modbus"
    "github.com/timzifer/quarc/config"
    runtimeReaders "github.com/timzifer/quarc/runtime/readers"
)

func main() {
    readerFactory := modbusdriver.NewReaderFactory(nil) // uses the default TCP client

    cfg := config.ReadGroupConfig{
        ID:       "temperature", 
        Function: "holding_registers",
        Start:    100,
        Length:   2,
        Endpoint: config.EndpointConfig{
            Address: "192.0.2.100:502",
            UnitID:  1,
            Timeout: config.Duration{Duration: 3 * time.Second},
        },
        Signals: []config.ReadSignalConfig{{
            Cell:  "zone.temperature",
            Type:  config.ValueKindFloat,
            Offset: 0,
            Scale: 0.1,
            Signed: true,
        }},
    }

    deps := runtimeReaders.ReaderDependencies{ /* obtain cells/activity trackers from Quarc */ }
    group, err := readerFactory(cfg, deps)
    if err != nil {
        panic(err)
    }
    defer group.Close()

    // Drive the group's lifecycle from your scheduler.
    ticker := time.NewTicker(time.Second)
    for {
        select {
        case now := <-ticker.C:
            if group.Due(now) {
                group.Perform(now, runtimeReaders.NoopLogger{})
            }
        case <-context.Background().Done():
            return
        }
    }
}
```

Replace the dependency stubs with the components supplied by your Quarc runtime instance.

## Configuration

### Endpoint configuration

`NewTCPClientFactory` expects a `config.EndpointConfig` that provides:

- `Address` – TCP endpoint in `host:port` form (required).
- `UnitID` – Modbus slave identifier.
- `Timeout` – optional request timeout (defaults to five seconds when unset or zero).

You can provide your own `ClientFactory` to support alternative transport layers. The factory must return something that satisfies the driver's `Client` interface (`ReadCoils`, `WriteSingleRegister`, etc.).

### Read groups

Use `config.ReadGroupConfig` plus optional driver-specific JSON overrides to describe how Modbus data is ingested:

```json
{
  "id": "switches",
  "function": "discrete_inputs",
  "start": 0,
  "length": 16,
  "driver_settings": {
    "function": "coils",
    "start": 8,
    "length": 8
  },
  "signals": [
    { "cell": "line.start", "type": "bool", "offset": 0 },
    { "cell": "line.stop",  "type": "bool", "offset": 1 }
  ]
}
```

The JSON within `driver_settings` is decoded by `resolveReadGroup`, letting you override the protocol function, start address, and length without duplicating the full Quarc config. Signal-level options include:

- `offset` – word or bit offset (depending on function type).
- `bit` – optional bit selection for register-based booleans.
- `endianness` – `"big"` (default) or `"little"` for register decoding.
- `scale` and `signed` – numeric conversions for integers, floats, and decimals.

### Write targets

Write targets mirror read group behaviour with additional controls:

```json
{
  "id": "setpoint",
  "cell": "zone.temperature_setpoint",
  "function": "holding_register",
  "address": 200,
  "deadband": 0.5,
  "rate_limit": "2s",
  "driver_settings": {
    "scale": 10,
    "signed": true,
    "endianness": "little"
  }
}
```

`resolveWriteTarget` applies JSON overrides for function, address, endianness, signedness, deadband, rate limit, and scaling. The writer automatically compares the incoming cell value against the last write using deadbands that respect the cell's type (float, integer, decimal, boolean). Rate limiting prevents re-issuing writes inside the configured interval.

### Modbus CUE overlay

The package registers a default overlay at `cue.mod/pkg/quarc.dev/quarc/drivers/modbus/modbus.cue`. Import it in your module to gain typed driver settings:

```cue
package example

import (
    "quarc.dev/quarc/module"
    modbus "quarc.dev/quarc/drivers/modbus"
)

myRead: modbus.#Read & {
    driver_settings: {
        function: "holding_registers"
        start:    100
        length:   4
    }
}

myWrite: modbus.#Write & {
    driver_settings: {
        function:   "coil"
        address:    10
        endianness: "little"
        signed:     false
    }
}
```

When the Quarc runtime loads your module, the overlay defined in `modbus_overlay.go` ensures these fields exist without extra scaffolding.

## Testing

Run the full test suite with:

```bash
go test ./...
```

The tests cover configuration resolution and schema registration helpers. Use `-run` and `-v` flags to focus on specific behaviours while developing new features.

## Troubleshooting tips

- **Connection failures** – ensure the Modbus endpoint is reachable and `Endpoint.Address` includes a port. The default timeout is five seconds; raise it via `Endpoint.Timeout` for slower PLCs.
- **Unsupported functions** – both readers and writers validate the Modbus function string. Supported values include `coil`, `discrete_inputs`, `holding_registers`, and `input_registers` (case-insensitive).
- **Out-of-range offsets** – register offsets are validated against the returned payload length. Double-check `length`, per-signal `offset` values, and scaling factors when you see "offset out of range" errors.
- **Deadband not respected** – remember that `deadband` defaults to zero. For floats and integers the difference must exceed the threshold to trigger a write; decimals compare using absolute difference.

## Contributing

1. Fork the repository and create a feature branch.
2. Add tests for any new behaviour.
3. Run `go fmt`, `go test ./...`, and ensure linting passes in your environment.
4. Submit a pull request describing your change.

Issues and feature requests are welcome!
