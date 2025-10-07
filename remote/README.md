# Remote Client Interfaces

The `remote` package abstracts Modbus connectivity behind the `Client` interface.
Implementations are expected to provide the subset of Modbus functions required
by the service and to be safe for concurrent use by the scheduler.

## Usage

```go
factory := remote.NewTCPClientFactory()
client, err := factory(endpointConfig)
if err != nil {
    log.Fatal().Err(err).Msg("failed to create client")
}
defer client.Close()

response, err := client.ReadHoldingRegisters(0, 4)
if err != nil {
    log.Warn().Err(err).Msg("read failed")
}
```

## Implementation Notes

* `ClientFactory` instances must return fully initialised clients. Expensive
  connection retries inside the factory will block configuration loading.
* Ensure the underlying `Client` implementation serialises Modbus requests per
  connection. The scheduler may call the interface from multiple goroutines.

## Common Pitfalls

* Forgetting to configure timeouts on transport handlers can lead to stalled
  goroutines and cascading back-pressure in the service.
* Returning a partially initialised client from the factory makes subsequent
  read/write operations fail in unpredictable ways.
