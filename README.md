![QUARC Gopher Mascot](docs/assets/quarc-gopher.svg)

# QUARC (Quadriphasic Automation Runtime Controller)

**CAUTION: Although functional and well written, this software has mainly been coded by openAI-Codex**

![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/timzifer/quarc/main/docs/coverage-badge.json)

QUARC is a deterministic, cyclic automation runtime that executes four strictly separated phases—**READ**, **PROGRAM**, **EVAL**, and **COMMIT**—on a fixed schedule. The controller stores typed in-memory cells, lets reusable control programs derive new signals, evaluates expression-based logic, and finally commits changes back to configured outputs using a write-on-change strategy. Protocol support is provided by pluggable drivers so deployments can run without physical IO and attach only the transports they require.

## Highlights

* Deterministic cycle scheduler with isolated read/program/eval/commit phases.
* Typed cell store with diagnostic propagation and manual override helpers.
* Configurable expression DSL with validation hooks and runtime logging.
* Transport-agnostic driver architecture with reusable bundles.
* Optional live view UI and Prometheus-compatible telemetry exporter.

## Documentation

* [Runtime overview](docs/runtime-overview.md)
* [Configuration guide](docs/configuration.md)
* [Reusable programs](docs/programs.md)
* [Driver integration](docs/drivers.md)
* [Expression DSL reference](docs/expression-dsl.md)
* [Live view and telemetry](docs/live-view.md)
* [Random reader driver](docs/random-reader.md)

## Getting started

1. Build/install the binary:
   ```bash
   go build ./cmd/...
   ```
2. Start the controller with your configuration:
   ```bash
   ./quarc --config path/to/config.cue
   ```

   Use `--config-check` to produce a detailed logic validation report without starting the service, or `--healthcheck` to perform a lightweight configuration validation suitable for container health probes.

## Continuous integration

The [Go coverage workflow](.github/workflows/coverage.yml) runs `go test ./... -coverprofile=coverage.out`, publishes the coverage profile as a build artifact, and reports the aggregated coverage percentage in the job summary.

## Release process

Releases tag the root module **and** the driver modules so that consumers can pin compatible versions. When creating a new version, create matching tags for:

* `github.com/timzifer/quarc`
* `github.com/timzifer/quarc/drivers/modbus`
* `github.com/timzifer/quarc/drivers/canstream`
* `github.com/timzifer/quarc/drivers/bundle`

This ensures downstream users embedding the drivers can resolve consistent module versions.

## Testing

Run the full test suite with:

```bash
go test ./...
```
