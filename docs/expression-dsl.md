# Expression DSL

QUARC compiles expressions with [`expr`](https://github.com/expr-lang/expr). The following helpers are available within logic and validation blocks:

| Function | Description |
|----------|-------------|
| `value(id)` | Returns the typed value of the referenced cell. Evaluation fails with a diagnostic if the cell is missing or invalid. |
| `cell(id)` | Alias for `value(id)` that is also available inside validation expressions. |
| `valid(id)` | Returns `true` when the referenced cell exists and is currently valid. |
| `dump(value)` | Logs the provided value at debug level and returns it unchanged. Useful for inspecting intermediate results. |
| `log(level?, message?, values...)` | Emits a log entry annotated with the current block/helper context. When the first argument is a recognised level (`trace`, `debug`, `info`, `warn`, `error`) it controls the severity; otherwise it is treated as the message. Remaining arguments are logged as `value`/`values`. |

Validation expressions (`valid` and `quality`) receive extra helpers: `value` (the computed result), `error` (a map with `code`/`message` for evaluation failures) alongside `error_raw`, `error_code`, and `error_message` convenience aliases. All regular DSL helpers are also available.

Expression ASTs execute only when all declared dependencies exist and are valid. Helper functions follow the same contract and return their result directly; any runtime errors bubble up to the caller.
