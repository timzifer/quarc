# Random Read Driver

The random read driver is a lightweight example implementation of a QUARC reader.
It emits pseudo-random values for every configured signal and demonstrates how a
reader integrates with the runtime cell store and signal buffer subsystem.

## Registration

```go
import (
        "github.com/timzifer/quarc/drivers/random"
        "github.com/timzifer/quarc/service"
)

svc, err := service.New(cfg, logger,
        service.WithReaderFactory("random", random.NewReadFactory()),
)
```

Add the driver to your configuration by setting the endpoint `driver` to
`"random"`. The driver reads its settings from the `driver_settings` section of a
read group.

## Configuration

```cue
reads: [
    {
        id: "demo",
        endpoint: {
            driver: "random"
        },
        ttl: "750ms",
        signals: [
            {
                cell: "temperature",
                type: "number",
            },
            {
                cell: "heater_enabled",
                type: "bool",
            },
        ],
        driver_settings: {
            source: "pseudo",      // pseudo, mersenne (alias) or secure
            seed: 1337,             // optional deterministic seed
            defaults: {
                min: 0,
                max: 100,
                true_probability: 0.5,
                quality: 0.95,
            },
            signals: {
                temperature: {
                    min: 15,
                    max: 25,
                },
                heater_enabled: {
                    true_probability: 0.2,
                },
            },
        },
    },
]
```

### Settings

| Field | Description |
|-------|-------------|
| `source` | Selects the random source. `pseudo`/`mersenne` uses Go's deterministic PRNG (with optional `seed`). `secure` uses `crypto/rand` for cryptographically strong randomness. |
| `defaults` | Optional default `SignalSettings` applied to every signal. |
| `signals` | Map of per-signal overrides keyed by the signal cell identifier. |

### Signal settings

| Field | Description |
|-------|-------------|
| `min` / `max` | Floating-point bounds for `number`, `float` and `decimal` signals. |
| `int_min` / `int_max` | Inclusive bounds for `integer` signals. |
| `true_probability` | Probability of generating `true` for boolean signals (defaults to `0.5`). |
| `string_length` | Length of generated strings (defaults to 12). |
| `alphabet` | Optional rune set used for string generation. |
| `quality` | Constant quality attached to the generated samples. |

Signals using unsupported value kinds (for example dates) are marked invalid and
report a diagnostic entry to aid debugging.

## Implementation notes

* Each read group instance owns its random source and schedules the next run
  according to the configured TTL.
* Generated samples are written to the target cells and pushed into the
  configured signal buffers so downstream aggregations work as expected.
* Buffer overflows are logged at `WARN` level while other failures increment the
  read error count returned to the scheduler.
