# QUARC MQTT Driver

This module provides reader and writer factories that plug into the [QUARC](https://github.com/timzifer/quarc) runtime. The driver keeps the QUARC core free from MQTT dependencies while supporting rich configuration options, buffered signal ingestion, and Home Assistant discovery.

## Registration

Add the driver factories when constructing a `service.Service` instance. Both factories live in the root package of this repository.

```go
import (
    mqttdriver "github.com/quarc-labs/quarc_driver_mqtt"
    "github.com/timzifer/quarc/service"
)

svc, err := service.New(
    ctx,
    cfg,
    service.WithDriver("mqtt", service.DriverFactories{
        Reader: mqttdriver.NewReadFactory(),
        Writer: mqttdriver.NewWriteFactory(),
    }),
)
```

Each QUARC read group or write target that references `driver.name: "mqtt"` must provide a `driver.settings` block that follows the schema documented below.

## Configuration overview

Driver settings are decoded from the `driver.settings` value of each read group or write target. Settings are independent per group/target so that different brokers or credentials can be used side by side.

### Reader settings

```jsonc
{
  "connection": {
    "broker": "tcp://broker:1883",
    "client_id": "quarc-reader",
    "keep_alive": "30s",
    "auth": { "username": "demo", "password": "demo" },
    "tls": {
      "enabled": true,
      "ca_file": "./ca.pem"
    }
  },
  "default_qos": 1,
  "payload": { "encoding": "json", "value_type": "float" },
  "buffer_defaults": { "capacity": 64 },
  "subscriptions": [
    {
      "topic": "sensors/temperature",
      "cell": "cell.temperature",
      "buffer": { "capacity": 64 }
    }
  ]
}
```

* `connection` – broker URL, authentication, TLS, and optional last will.
* `subscriptions` – list of topic to cell bindings. Each entry may override QoS or payload conversion.
* `payload` – default decoder if a subscription does not specify its own conversion (supports JSON extraction, string and binary payloads).
* `buffer_defaults` / `subscription.buffer` – optional overrides for the configured QUARC signal buffers. The driver validates the declared capacity against the buffer created by `buildReadGroups` to prevent configuration drift.
* `subscriptions[].home_assistant` – optional Home Assistant integration publishing discovery and availability information so Home Assistant entities can update QUARC cells.

The standard QUARC read-group `signals` block must still register buffers and aggregations. Aggregated cells are flushed by the core runtime during the COMMIT phase; the MQTT driver pushes samples into the configured buffers via `readers.SignalBuffer`.

When `home_assistant` is enabled on a subscription, the driver publishes a retained discovery payload and availability state for the cell. Home Assistant can then send commands to the configured `command_topic` (defaults to the subscription topic) to change the cell value. Configure `state_topic` when Home Assistant should observe state updates on a dedicated topic to avoid command echoes.

```jsonc
"home_assistant": {
  "enabled": true,
  "component": "number",
  "object_id": "setpoint",
  "name": "HVAC Setpoint",
  "command_topic": "home/hvac/setpoint/set",
  "state_topic": "home/hvac/setpoint/state"
}
```

### Writer settings

```jsonc
{
  "connection": {
    "broker": "tls://broker:8883",
    "client_id": "quarc-writer"
  },
  "topic": "actuators/relay/state",
  "qos": 1,
  "retain": true,
  "payload": { "encoding": "json" },
  "deadband": { "absolute": 0.2 },
  "rate_limit": { "min_interval": "10s" }
}
```

* `topic` – outbound MQTT topic.
* `payload` – encoder used for outbound values. Defaults to JSON.
* `deadband` / `rate_limit` – optional publication throttling.

When the optional `home_assistant` section is enabled the writer publishes discovery payloads, availability topics, and can mirror state data to a dedicated Home Assistant topic. Discovery uses the driver connection's client ID to form a unique identifier and defaults the discovery prefix to `homeassistant`.

```jsonc
"home_assistant": {
  "enabled": true,
  "discovery_prefix": "ha",
  "component": "sensor",
  "object_id": "garage_temp",
  "name": "Garage Temperature",
  "availability": { "retain": true },
  "device": {
    "identifiers": ["garage-controller"],
    "manufacturer": "QUARC Labs"
  }
}
```

## Lifecycle

* **READ phase** – The read factory creates an MQTT client per read group, subscribes on connect, and pushes decoded payloads into the corresponding `state.Cell` and optional signal buffers. Overflow is reported via the QUARC logger and surfaced as a buffer diagnostic during the COMMIT phase.
* **PROGRAM/EVAL phases** – The driver does not participate.
* **COMMIT phase** – QUARC flushes signal buffers and applies aggregation policies. Home Assistant writers mark the entity as online when a valid value is available and publish discovery information exactly once.
* **Shutdown** – `Close()` disconnects the MQTT session. Writers additionally send an offline availability payload if Home Assistant mode is enabled.

## Troubleshooting

* Ensure the `driver.settings.connection.broker` URL uses the correct scheme (`tcp`, `ssl`, or `ws`).
* Buffer capacity mismatches between the driver settings and the QUARC signal configuration will cause the factory to fail fast with a descriptive error.
* Payload conversion errors are logged per message with the offending topic to aid debugging.
* In Home Assistant mode discovery will only be published once per process; remove the retained discovery topic when experimenting with different metadata.

## Schemas and examples

The `config/mqtt_driver.cue` file documents the CUE schema for both reader and writer settings. Example overlays for standard and Home Assistant usage are located in `config/examples/`.
