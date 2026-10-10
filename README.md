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

## Scope record migration

`scope` now contains an opaque `registry : scope_registry` field. Existing
function signatures are unchanged. Prefer `Signal.scope name` or
`Signal.make_scope name`; code that manually initializes the record must add
`registry = Signal.make_scope_registry ()` with a fresh registry for each scope.
Keep the existing unique scope id and fresh reference fields. Do not share
registries between scopes or mutate lifecycle record fields directly.

`cleanup_callbacks` and `owned_subscriptions` retain their list types, but may
temporarily contain cancelled empty nodes. Replace
`List.length !(sc.cleanup_callbacks)` with `Signal.scope_cleanup_count sc`
(constant time), and use `Signal.scope_owned_count sc` for live owned handles
(a linear scan). Cleanup cancellation immediately releases the callback's
captured resources. Removing a switch or keyed lifetime similarly clears the
owner's captured collection. Empty nodes are compacted when half of the stored
registrations have been cancelled, avoiding a full list copy on every removal.
The cleanup list contains at most twice as many nodes as live registrations,
and registry metadata shrinks with that live count. Ordinary subscriptions passed
to `own` remain owned until scope disposal; cancelling the original subscription
does not by itself remove its owner node. `scope_owned_count` excludes it.

Subscriber failures notify the remaining live subscribers before propagating
the first exception; call `stabilize` again to drain queued dependent work.
Nested `stabilize` calls on the same scheduler leave work to the outer flush.
Keyed callback failures finish resource accounting before propagating the first
exception, but external patch side effects cannot be rolled back: patch consumers
must resynchronize after a failed callback. Factories that raise before returning
a scope are responsible for releasing their own unreturned resources.

## Property testing and performance

`dune runtest` runs the regression suite and 19 QCheck2 properties with seed
24301: 7,140 generated cases covering staged writes, random DAGs and cutoffs,
exception recovery, cancellation, scope/switch/slot lifetimes, keyed patch replay
and identity, duplicate keys, and clamped list operations. Generators shrink
failing inputs, and the reported seed makes failures reproducible.

Properties also check exact recomputation/task counts, linear allocation budgets
for queues, fanout, subscriptions, and scope lifetimes, and an `n log n`
comparison budget for keyed reversal. CI runs a longer fixed-seed suite plus a
seed derived from the workflow run ID. Portable lifecycle/exception regressions
also run as bytecode and Melange JavaScript under Node. Native GC probes check
that cancelled callbacks and collections release captured resources, and
allocation ratios check bulk child and collection lifetime cancellation.
QCheck is a test-only dependency.

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
