# ocaml-signal

A small statically typed incremental runtime for OCaml: reactive values,
scoped state, lifecycle, effects, switches, and keyed collections.

A conventional OCaml library: create a scheduler, feed it states, derive
signals, and let `stabilize` flush staged changes to subscribers.

## Concepts

- **Scheduler** — each signal belongs to one scheduler. Mutation is staged
  through dirty tasks and effects and applied by `Signal.stabilize`, which
  runs to a fixpoint and reports per-round diagnostics.
- **Signals** — `constant`, `state`, `set`, `update`, `sample`, `observe`,
  `subscribe`, `dispose_signal`, derived `map`/`map2`, and `cutoff` for
  equality-bounded propagation.
- **Scopes** — `scope`, `make_scope`, `child_scope`, `mount`,
  `dispose_scope`, `on_mount`, `on_unmount`, `on_dispose`,
  `register_cleanup`, `own`, `own_signal`, and `state_at` slots scoped to a
  scope id.
- **Switch** — `switch` swaps a mounted child scope as a signal's value
  changes, disposing the previous one.
- **Keyed collections** — `keyed` reconciles a collection of items by key,
  mounts a scope per item, and reports `Insert`/`Remove`/`Move` patches.

See `src/signal.mli` for the documented API.

## Building

```sh
opam install . --deps-only --with-test
dune build
dune runtest
```

The library builds for native, bytecode, and Melange.

## Property testing and performance

`dune runtest` runs the regression suite and 17 QCheck2 properties with seed
24301: 6,140 generated cases covering staged writes, random DAGs and cutoffs,
exception recovery, cancellation, scope/switch/slot lifetimes, keyed patch replay
and identity, duplicate keys, and clamped list operations. Generators shrink
failing inputs, and the reported seed makes failures reproducible.

Properties also check exact recomputation/task counts, linear allocation budgets
for queues, fanout, subscriptions, and scope lifetimes, and an `n log n`
comparison budget for keyed reversal. CI runs a longer fixed-seed suite plus a
seed derived from the workflow run ID. QCheck is a test-only dependency.

```sh
dune exec test/test_properties.exe -- --long --seed 24301 --no-colors
dune exec test/test_properties.exe -- --seed 12345 --no-colors
dune exec --profile release test/bench_signal.exe
```

The native benchmark reports median elapsed time and allocated words for FIFO
work, wide/deep graphs, subscriptions, and keyed reversal, using three warmups
and nine samples per size. It excludes graph construction and collects garbage
before each measurement. Timing is reported separately from the CI properties;
there are no machine-dependent timing thresholds. These measurements concern
native OCaml, not Melange JavaScript execution.
