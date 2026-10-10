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

## Internal representation

All record types are abstract in `src/signal.mli`; lifecycle state is exposed
through accessors (`scope_name`, `scope_is_disposed`, `scope_cleanup_entries`,
`scope_owned_entries`, `switch_scope`, `switch_is_disposed`,
`switch_subscription`, `keyed_entries`, `keyed_is_disposed`,
`make_keyed_entry`/`keyed_entry_*`, `state_slot_count`,
`subscription_disposed`, `signal_is_disposed`). Use
`Signal.scope_cleanup_count sc` (constant time) and
`Signal.scope_owned_count sc` (a linear scan) for live counts — the entry
lists may temporarily contain cancelled nodes until the next compaction.

Scopes keep no per-scope table: each cleanup entry shares its `disposed`
reference with its cancellation handle, and the registry stores a handful of
counters (`live_cleanups`, `cleanup_dead`, `owned_slots`, `owned_dead`,
`owned_scan`). Cleanup cancellation immediately releases the callback's
captured resources and counts a dead node; dead nodes are compacted when they
outnumber live ones, so the cleanup list stays within twice the live
registrations. `own` returns a counted handle so cancellations shrink the same
bounded structure; handles cancelled outside their owned wrapper are swept
when the stored list passes a scan watermark. Removing a switch or keyed
lifetime clears the owner's captured collection.

Subscriber, switch, and keyed notifications run in an observer phase after
all computations in the round settle, so observers always read a consistent
graph; a `set` staged inside an observer runs in the next round. Subscriber
failures notify the remaining live observers before propagating the first
exception. A nested `stabilize` on the same scheduler drains the queues
synchronously — event handlers can flush queued writes before reading
derived state.

Derived `map`/`map2`/`cutoff` nodes are tracked by necessity: when the last
live subscriber cancels, the node releases its upstream subscriptions and
stops recomputing; subscribing again reconnects it and recomputes on demand.
Keep derived nodes owned (`own_signal`) or scoped so their lifetime matches
their readers.

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
