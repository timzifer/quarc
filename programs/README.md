# Program Interfaces

The `programs` package contains the plug-in system for control logic. Concrete
programs implement the `Program` interface and are instantiated through
registered `Factory` functions.

## Usage

```go
program, err := programs.Instantiate("pid", "loop1", settings)
if err != nil {
    return err
}
outputs, err := program.Execute(ctx, inputs)
if err != nil {
    return err
}
process(outputs)
```

## Implementation Notes

* `Execute` is called on every scheduler cycle; avoid expensive blocking
  operations and ensure deterministic behaviour for a given input snapshot.
* Use the supplied `Context` to compute deltas or time-based logic rather than
  querying the clock directly.
* Register factories during init using `programs.RegisterProgram` with a stable
  identifier so configuration reloads can reconstruct instances.

## Common Pitfalls

* Returning shared slices or signal structs without copying can cause data races
  when the scheduler mutates them for downstream programs.
* Ignoring errors from helper functions when parsing `settings` often results in
  confusing runtime panics; validate configuration eagerly inside the factory.
