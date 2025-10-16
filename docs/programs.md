# Reusable Programs

Programs encapsulate reusable control algorithms that run during the PROGRAM phase. Examples include PID regulators, ramp generators, slew limiters, timers, counters, filters, selection logic, latches, runtime/energy tracking, sequencers, and alarm supervision.

Each program entry declares:

* `id` – Unique instance identifier referenced by diagnostics.
* `type` – Program factory key (e.g. `pid`, `ramp`, `slew_asym`).
* `inputs` / `outputs` – Signal maps from program channels to cell IDs. Optional signals can define defaults and type overrides.
* `settings` – Arbitrary key/value map forwarded to the program factory for tuning parameters.
* `metadata` – Free-form JSON payload made available to program implementations.

Programs execute before logic evaluation and write directly into their associated cells, making the results available to logic blocks and downstream drivers. See [`config.example.cue`](../config.example.cue) for examples of bundled programs and wiring patterns.
