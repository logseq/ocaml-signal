(** A small statically typed incremental runtime.

    Reactive values, scoped state, lifecycle, effects, switches, and keyed
    collections. Each ['value signal] stores only callbacks accepting that same
    ['value] type; the scheduler stores only zero-argument tasks.

    {b Single domain.} Schedulers, signals, and scopes are ordinary mutable
    state and must be used from one domain at a time. Scope ids are allocated
    with [Atomic], so creating scopes on two domains does not duplicate ids,
    but nothing else in the graph is synchronized.

    {b Native and JavaScript.} Physical equality ([(==)] / [(!=)]) is
    identity on native OCaml. Melange compiles it to JavaScript [===] / [!==],
    which compare strings and numbers by value. Keyed item updates use [(!=)],
    so an equal-but-fresh string or float may republish on native and not on
    JavaScript. The final value is the same. Prefer {!phys_equal} or
    [Float.equal] over structural [(=)], which treats every [NaN] as different
    and raises [Invalid_argument] on closures. *)

(** {1 Scheduler} *)

(** Counters from the last {!stabilize} that ran to completion.
    [stabilization_dirty_tasks] counts dirty-queue tasks and derived
    computations. Rank-only rounds are included in [stabilization_rounds] but
    do not consume the runaway cap; that cap counts rounds that ran effects or
    dirty tasks. *)
type stabilization_diagnostics = {
  stabilization_generation : int;
  stabilization_rounds : int;
  stabilization_effects : int;
  stabilization_dirty_tasks : int;
}

(** Owns the effect, dirty-task, and computation queues shared by every signal
    created through it. Signals from different schedulers must not be combined. *)
type scheduler

(** A cancellable handle returned by {!observe}, {!subscribe}, {!own}, and
    {!register_cleanup}. *)
type subscription

(** {1 Signals and states} *)

(** A reactive value. Reading a derived signal with subscribers does not
    recompute it. A derived signal with no subscribers is detached from its
    inputs; {!sample} refreshes it from the inputs' current values and does
    not see writes that have not been published by {!stabilize}. *)
type 'value signal

(** Mutable input. {!set} and {!update} stage a pending value that is
    published on the next {!stabilize}. Publishing the same value again is
    intentional: states have no equality cutoff. Use {!cutoff} to drop
    unchanged derived values. *)
type 'value state

(** {1 Scopes} *)

(** Lifecycle boundary for subscriptions, signals, cleanups, and per-scope
    state slots. Disposing a scope disposes everything it owns.
    Cleanups run FIFO. A child scope runs at the cleanup registered for it,
    before later cleanups. Owned subscriptions run after cleanups. Unmount
    callbacks run last, including when the scope was never mounted.
    Disposal walks an explicit stack, so a deep tree does not grow the
    native or JavaScript call stack. *)
type scope

(** A named slot template. {!state_at} materializes one ['value state] per
    scope id, removed when the scope is disposed. *)
type 'value state_slot

(** {1 Switch} *)

(** One mounted scope for the current key of a signal. *)
type 'key switch

(** {1 Keyed collections} *)

(** A structural patch emitted by {!val-keyed} reconciliation. *)
type 'key keyed_patch =
  | Insert of 'key * int
  | Remove of 'key * int
  | Move of 'key * int * int

(** One reconciled item. Constructed by {!val-keyed} and {!reconcile_keyed}. *)
type ('key, 'item) keyed_entry

(** A reconciled collection with one scope per live item. *)
type ('key, 'item) keyed

(** {1 Scheduler operations} *)

(** [scheduler ()] creates a fresh scheduler with an empty work queue. *)
val scheduler : unit -> scheduler

(** [generation owner] is how many {!stabilize} calls on [owner] performed work.
    A nested call that itself performs work increments the counter too. *)
val generation : scheduler -> int

(** [last_stabilization owner] is the diagnostics of the most recent
    {!stabilize} that completed. A run that raises leaves this unchanged. *)
val last_stabilization : scheduler -> stabilization_diagnostics

(** [enqueue_effect owner f] appends [f] to the effect queue. The next
    {!stabilize} runs effects before dirty tasks of the same round. *)
val enqueue_effect : scheduler -> (unit -> unit) -> unit

(** [enqueue_dirty owner task] appends [task] to the dirty queue. *)
val enqueue_dirty : scheduler -> (unit -> unit) -> unit

(** [schedule_once owner scheduled task] enqueues [task] only when [scheduled]
    is [false], then sets it. The wrapper clears it before running [task], so
    a task may reschedule itself. *)
val schedule_once : scheduler -> bool ref -> (unit -> unit) -> unit

(** Raised when effect or dirty-task rounds do not reach a fixpoint within
    {!max_stabilization_rounds}. The payload is [(cap, effects run, dirty tasks
    and computations run)]. A deep acyclic chain of derived signals does not
    consume this cap. *)
exception Stabilization_limit_exceeded of int * int * int

(** Maximum effect/dirty rounds before {!Stabilization_limit_exceeded}. *)
val max_stabilization_rounds : int

(** [stabilize owner] drains work to a fixpoint.
    Each round runs a snapshot of effects, then dirty tasks. Derived
    computations then run one rank at a time, low rank first. After every
    rank has settled, user subscribers and switch/keyed callbacks run. Each
    of those callbacks has its own exception handler; the first exception is
    re-raised, with its backtrace, after the rest have run. Writes performed
    by a callback wait for the next round.
    A nested call on the same scheduler runs synchronously: queued owner
    writes are published before it returns. If a nested call has already
    taken a task, the outer snapshot stops instead of raising [Queue.Empty].
    A computation that raises is queued again, so a later {!stabilize}
    retries it without another upstream {!set}. Tasks that had not started
    stay queued. On success the generation advances when any task ran.
    @raise Stabilization_limit_exceeded on a runaway effect or dirty task. *)
val stabilize : scheduler -> unit

(** {1 Signal and state operations} *)

(** [constant owner initial] creates a signal that never changes. *)
val constant : scheduler -> 'value -> 'value signal

(** [state owner initial] creates a mutable state and its backing signal. *)
val state : scheduler -> 'value -> 'value state

(** [value st] is the signal backing [st]. *)
val value : 'value state -> 'value signal

(** [sample sig] reads the current value. For a derived signal that nobody
    subscribes to, this refreshes it from its inputs' published values. *)
val sample : 'value signal -> 'value

(** [get sig] is {!sample}. *)
val get : 'value signal -> 'value

(** [get_state st] is {!get} of [value st]. *)
val get_state : 'value state -> 'value

(** [set st v] stages [v]. A {!set} on a disposed state is ignored and does
    not enqueue work. *)
val set : 'value state -> 'value -> unit

(** [update st f] stages [f] applied to the pending value, or the current
    value when nothing is pending. *)
val update : 'value state -> ('value -> 'value) -> unit

(** [subscribe ?emit_initial sig callback] registers [callback].
    When [emit_initial] is [true] (the default) the callback runs immediately
    with the current value. If that call raises, the registration is cancelled
    first.
    The callback runs during {!stabilize}, after derived values in that wave
    have been published, not in the middle of propagation. If it raises, every
    other user callback for the wave still runs, then the first exception is
    re-raised with its backtrace.
    Raises [Invalid_argument] on a disposed signal. *)
val subscribe :
  ?emit_initial:bool -> 'value signal -> ('value -> unit) -> subscription

(** [observe sig callback] is [subscribe ~emit_initial:true]. *)
val observe : 'value signal -> ('value -> unit) -> subscription

(** [dispose_subscription sub] cancels [sub]. Idempotent.
    Cancelling a subscription returned by {!own} also drops it from the scope. *)
val dispose_subscription : subscription -> unit

(** [make_subscription cancel] is a handle that runs [cancel] once, on
    {!dispose_subscription}. Hosts use it for cancellable work that is not a
    signal subscription. *)
val make_subscription : (unit -> unit) -> subscription

(** [subscription_disposed sub] is [true] after [sub] has been cancelled,
    including when its signal is disposed. *)
val subscription_disposed : subscription -> bool

(** [dispose_signal sig] detaches [sig] from its inputs and drops its
    subscribers. Idempotent. Later {!observe} and {!subscribe} raise
    [Invalid_argument]. *)
val dispose_signal : 'value signal -> unit

(** [map f sig] derives [f] applied to each published value of [sig].
    The derivation subscribes to [sig] only while something subscribes to the
    result. After the last subscriber is cancelled, [f] is not called again
    until the next {!sample} or a new subscriber. The initial value is computed
    immediately. *)
val map : ('left -> 'output) -> 'left signal -> 'output signal

(** [map2 f left right] derives [f] applied to the latest settled values of
    both inputs. Both signals must share one scheduler. The same subscriber
    rule as {!map} applies.
    Raises [Invalid_argument] when the schedulers differ or either input is
    already disposed. *)
val map2 :
  ('left -> 'right -> 'output) -> 'left signal -> 'right signal -> 'output signal

(** [cutoff equal sig] republishes only when [equal old new] is [false].
    Pass {!phys_equal} for values that may be [NaN] or closures. Structural
    [(=)] is not a safe default: [NaN] never compares equal, and comparing
    closures raises [Invalid_argument]. *)
val cutoff : ('value -> 'value -> bool) -> 'value signal -> 'value signal

(** Physical equality, [(==)]. Does not raise on closures. The same [Float.nan]
    value compares equal. See the native/JavaScript note above. *)
val phys_equal : 'value -> 'value -> bool

(** {1 Scope operations} *)

(** [scope name] creates an unmounted root scope. *)
val scope : string -> scope

(** [child_scope name parent] creates an unmounted scope owned by [parent].
    Disposing [parent] disposes the child. The link is removed in constant time
    when the child is disposed on its own.
    Raises [Invalid_argument] when [parent] is disposed. *)
val child_scope : string -> scope -> scope

(** [make_scope name] is {!val-scope}. *)
val make_scope : string -> scope

(** Scope label passed to {!scope} or {!child_scope}. *)
val scope_name : scope -> string

(** Unique id. Ids are allocated with [Atomic]. *)
val scope_id : scope -> int

(** [scope_disposed sc] is [true] after {!dispose_scope} has started. *)
val scope_disposed : scope -> bool

(** [scope_mounted sc] is [true] after a successful {!mount} request, including
    while disposal is in progress only until disposal marks the scope. *)
val scope_mounted : scope -> bool

(** Live cleanup registrations. Constant time. *)
val scope_cleanup_count : scope -> int

(** Live owned handles. Constant time. *)
val scope_owned_count : scope -> int

(** [on_mount sc f] runs [f] when [sc] is mounted. If [sc] is already mounted,
    [f] runs immediately. A raising callback does not skip the others; the
    first exception is re-raised after the rest run. A callback that disposes
    [sc] does skip the rest. *)
val on_mount : scope -> (unit -> unit) -> unit

(** [on_unmount sc f] registers [f] to run when [sc] is disposed, whether or
    not {!mount} was called. *)
val on_unmount : scope -> (unit -> unit) -> unit

(** [on_dispose sc f] registers [f] as a cleanup. On an already disposed scope,
    [f] runs immediately. *)
val on_dispose : scope -> (unit -> unit) -> unit

(** [register_cleanup sc f] registers [f] and returns a handle that cancels it
    in constant time, including when an earlier cleanup disposes [sc].
    Cancellation drops [f] so captured values can be collected. *)
val register_cleanup : scope -> (unit -> unit) -> subscription

(** [own sc sub] ties [sub] to [sc]. Disposing [sc] cancels [sub]. Cancelling
    [sub] removes it from [sc]. On a disposed scope, [sub] is cancelled
    immediately. *)
val own : scope -> subscription -> subscription

(** [own_signal sc sig] disposes [sig] when [sc] is disposed. *)
val own_signal : scope -> 'value signal -> 'value signal

(** [mount sc] marks [sc] mounted and runs pending mount callbacks once. *)
val mount : scope -> unit

(** [dispose_scope sc] marks [sc] disposed and runs cleanups, owned
    subscriptions, and unmount callbacks to completion before returning.
    That includes a call made from a cleanup of a scope that is already
    being disposed, so a [Fun.protect] around it observes the finished
    scope. A {!child_scope} chain is still torn down on the heap stack and
    does not grow the native or JavaScript call stack. Idempotent. If a
    callback raises, the remaining callbacks of that disposal still run,
    then the first exception is re-raised with its backtrace. *)
val dispose_scope : scope -> unit

(** [active sc] is [true] while [sc] is mounted and not disposed. *)
val active : scope -> bool

(** [state_slot name] creates a slot template for {!state_at}. *)
val state_slot : string -> 'value state_slot

(** How many scopes currently hold a state in [slot]. *)
val state_slot_count : 'value state_slot -> int

(** [state_at owner sc slot initial] returns the per-scope state for [slot],
    creating it on first use. The entry is removed when [sc] is disposed.
    Raises [Invalid_argument] on a disposed scope, or when [slot] in [sc] was
    already created with a different scheduler. *)
val state_at : scheduler -> scope -> 'value state_slot -> 'value -> 'value state

(** {1 Switch operations} *)

(** [switch parent key_sig equal mount] mounts [mount key] for the current key.
    A later published key disposes that scope and mounts a new one when [equal]
    says the keys differ. The replacement runs after derived signals have
    settled, so [mount] observes up-to-date inputs.
    Disposing [parent] disposes the switch. Publications during construction
    are coalesced until the factory and mount callbacks return.
    Raises [Invalid_argument] on a disposed parent or source. If [mount] raises
    before returning a scope, that scope is not tracked. A mount callback that
    raises disposes the scope it returned; a later publication of the same key
    retries. *)
val switch :
  scope ->
  'key signal ->
  ('key -> 'key -> bool) ->
  ('key -> scope) ->
  'key switch

(** Scope currently held by the switch. *)
val switch_scope : 'key switch -> scope

(** [switch_disposed sw] is [true] after {!dispose_switch} or parent disposal. *)
val switch_disposed : 'key switch -> bool

(** Subscription that watches the switch's key. Disposing the source disposes
    this subscription. *)
val switch_subscription : 'key switch -> subscription

(** [dispose_switch sw] disposes the switch and its current scope. Idempotent. *)
val dispose_switch : 'key switch -> unit

(** {1 Keyed collection operations} *)

(** [reconcile_keyed owner entries items key_of compare mount on_patch]
    diffs [items] against [entries]. Removed keys are disposed and reported
    greatest index first. Kept keys move when their index changes. New keys
    are mounted and inserted.
    Retained payloads are compared with [(!=)]. See the native/JavaScript note.
    A failing mount callback releases that row and stops the walk, but rows
    already committed stay committed. The exception still propagates. {!keyed}
    schedules the same target again so the failed row is retried on the next
    {!stabilize} without another write. Patch callbacks that raise do not roll
    back earlier patches; consumers must resynchronize.
    Recursive reconciliation of the same [entries] list raises
    [Invalid_argument]. The guard lives on [owner], not in a global table.
    For [n = old_length + new_length], reconciliation uses [O(n log (n + 1))]
    comparisons and [O(n)] auxiliary storage, excluding callbacks.
    {!keyed_find_scope} indexes by the same comparator. This function does not
    build that index. *)
val reconcile_keyed :
  scheduler ->
  ('key, 'item) keyed_entry list ref ->
  'item list ->
  ('item -> 'key) ->
  ('key -> 'key -> int) ->
  ('item signal -> scope) ->
  ('key keyed_patch -> unit) ->
  unit

(** [keyed parent items_sig key_of compare mount on_patch] reconciles each
    published list. Live keys are stored in a tree ordered by [compare], so
    {!keyed_find_scope} is [O(log n)] even when equal keys do not share a
    polymorphic hash.
    Disposing [parent] disposes every item scope. A mount failure raises and
    retries that target on the next {!stabilize}. *)
val keyed :
  scope ->
  'item list signal ->
  ('item -> 'key) ->
  ('key -> 'key -> int) ->
  ('item signal -> scope) ->
  ('key keyed_patch -> unit) ->
  ('key, 'item) keyed

(** Live entries, in order. *)
val keyed_entries : ('key, 'item) keyed -> ('key, 'item) keyed_entry list

(** Key stored on an entry. *)
val entry_key : ('key, 'item) keyed_entry -> 'key

(** Scope mounted for an entry. *)
val entry_scope : ('key, 'item) keyed_entry -> scope

(** State holding the entry's latest item. *)
val entry_state : ('key, 'item) keyed_entry -> 'item state

(** [keyed_find_scope k key] is the scope for [key], using the collection's
    comparator. Raises [Invalid_argument] when [key] is not live.
    [O(log n)] in the number of live keys. *)
val keyed_find_scope : ('key, 'item) keyed -> 'key -> scope

(** [dispose_keyed k] cancels reconciliation and disposes item scopes.
    Idempotent. *)
val dispose_keyed : ('key, 'item) keyed -> unit
