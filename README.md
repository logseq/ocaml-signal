# ocaml-signal

A small statically typed incremental runtime for OCaml: reactive values,
scoped state, lifecycle, effects, switches, and keyed collections.

`ocaml-signal` is the plain-OCaml port of
[signal-lg](https://github.com/logseq/signal-lg). It keeps the same API and
semantics while exposing a conventional OCaml library instead of an LG
package.

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

See `src/signal.mli` for the full API.

## Building

```sh
opam install . --deps-only --with-test
dune build
dune runtest
```

The library builds for native, bytecode, and Melange.
