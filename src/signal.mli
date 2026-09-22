(** A small statically typed incremental runtime.

    Reactive values, scoped state, lifecycle, effects, switches, and keyed
    collections. Each ['value signal] stores only callbacks accepting that same
    ['value] type; the scheduler stores only zero-argument tasks. *)

(** {1 Scheduler} *)

(** Counters describing the work performed by the last {!stabilize} run. *)
type stabilization_diagnostics = {
  stabilization_generation : int;
  stabilization_rounds : int;
  stabilization_effects : int;
  stabilization_dirty_tasks : int;
}

(** Owns the effect and dirty-task queues shared by every signal created
    through it. Signals and scopes created on different schedulers must not be
    combined. *)
type scheduler = {
  effects : (unit -> unit) list ref;
  dirty : (unit -> unit) list ref;
  generation_value : int ref;
  last_stabilization_value : stabilization_diagnostics ref;
}

(** A cancellable handle returned by {!observe}, {!subscribe}, and
    {!register_cleanup}. *)
type subscription = { disposed : bool ref; cancel : unit -> unit }

(** A registered signal callback. *)
type 'value subscriber = { subscriber_id : int; callback : 'value -> unit }

(** A registered scope cleanup callback. *)
type cleanup_entry = { cleanup_id : int; cleanup_callback : unit -> unit }

(** {1 Signals and states} *)

(** A reactive value. Reading it never triggers recomputation; subscribers are
    notified when {!stabilize} publishes a new current value. *)
type 'value signal = {
  owner : scheduler;
  current : 'value ref;
  next_subscriber_id : int ref;
  subscribers : 'value subscriber list ref;
  upstream_subscriptions : subscription list ref;
  disposed_signal : bool ref;
}

(** Mutable input for the graph. {!set} and {!update} stage a pending value
    that is published on the next {!stabilize}. *)
type 'value state = {
  state_signal : 'value signal;
  pending : 'value option ref;
  scheduled : bool ref;
}

(** {1 Scopes} *)

(** Lifecycle boundary for subscriptions, signals, cleanups, and per-scope
    state slots. Disposing a scope disposes everything it owns. *)
type scope = {
  scope_id : int;
  scope_name : string;
  next_cleanup_id : int ref;
  cleanup_callbacks : cleanup_entry list ref;
  mount_callbacks : (unit -> unit) list ref;
  unmount_callbacks : (unit -> unit) list ref;
  owned_subscriptions : subscription list ref;
  mounted : bool ref;
  disposed_scope : bool ref;
}

(** A named slot template. {!state_at} materializes one ['value state] per
    scope id, removed when the scope is disposed. *)
type 'value state_slot = {
  slot_name : string;
  slot_states : (int, 'value state) Hashtbl.t;
}

(** {1 Switch} *)

(** Keeps exactly one mounted child scope alive for the current key of a
    signal. *)
type 'key switch = {
  switch_subscription : subscription;
  switch_scope : scope ref;
  switch_disposed : bool ref;
}

(** {1 Keyed collections} *)

(** A structural patch emitted by {!val-keyed} reconciliation: insert or remove an
    entry at an index, or move an entry between indices. *)
type 'key keyed_patch =
  | Insert of 'key * int
  | Remove of 'key * int
  | Move of 'key * int * int

(** One reconciled collection item: its key, backing state, and scope. *)
type ('key, 'item) keyed_entry = {
  entry_key : 'key;
  entry_state : 'item state;
  entry_scope : scope;
}

(** A reconciled collection with one scope per item. *)
type ('key, 'item) keyed = {
  keyed_subscription : subscription;
  keyed_entries : ('key, 'item) keyed_entry list ref;
  keyed_compare : 'key -> 'key -> int;
  keyed_disposed : bool ref;
}

(** {1 Scheduler operations} *)

(** [scheduler ()] creates a fresh scheduler with an empty work queue. *)
val scheduler : unit -> scheduler

(** [generation owner] is the number of {!stabilize} runs that performed work. *)
val generation : scheduler -> int

(** [last_stabilization owner] reports the diagnostics of the most recent
    {!stabilize} run. *)
val last_stabilization : scheduler -> stabilization_diagnostics

(** [enqueue_effect owner f] appends [f] to the effect queue, run by the next
    {!stabilize} before dirty tasks of the same round. *)
val enqueue_effect : scheduler -> (unit -> unit) -> unit

(** [enqueue_dirty owner task] appends [task] to the dirty queue. *)
val enqueue_dirty : scheduler -> (unit -> unit) -> unit

(** [schedule_once owner scheduled task] enqueues [task] as a dirty task only
    when [scheduled] is [false], setting it to [true]; the wrapper resets it to
    [false] before running [task], so a task may reschedule itself. *)
val schedule_once : scheduler -> bool ref -> (unit -> unit) -> unit

(** [stabilize owner] drains the effect and dirty queues to a fixpoint: each
    round runs all queued effects, then all dirty tasks, repeating until both
    queues are empty. Increments {!generation} and updates
    {!last_stabilization} only when at least one task ran. *)
val stabilize : scheduler -> unit

(** {1 Signal and state operations} *)

(** [constant owner initial] creates a signal that never changes. *)
val constant : scheduler -> 'value -> 'value signal

(** [state owner initial] creates a mutable state and its backing signal. *)
val state : scheduler -> 'value -> 'value state

(** [value st] is the signal backing state [st]. *)
val value : 'value state -> 'value signal

(** [sample sig] reads the current value without subscribing. *)
val sample : 'value signal -> 'value

(** [get st] is {!sample} of [value st]. *)
val get : 'value state -> 'value

(** [set st v] stages [v] as the next value; published on {!stabilize}. *)
val set : 'value state -> 'value -> unit

(** [update st f] stages [f] applied to the current value. *)
val update : 'value state -> ('value -> 'value) -> unit

(** [subscribe ?emit_initial sig callback] registers [callback] to run at
    every publish. When [emit_initial] is [true] (the default) the callback
    also runs synchronously with the current value.
    Raises [Invalid_argument] on a disposed signal. *)
val subscribe : ?emit_initial:bool -> 'value signal -> ('value -> unit) -> subscription

(** [observe sig callback] is [subscribe ~emit_initial:true]. *)
val observe : 'value signal -> ('value -> unit) -> subscription

(** [dispose_subscription sub] cancels a subscription; idempotent. *)
val dispose_subscription : subscription -> unit

(** [dispose_signal sig] disposes the signal, cancelling its subscribers and
    upstream subscriptions; idempotent.
    Raises [Invalid_argument] on subsequent {!observe}/{!subscribe}. *)
val dispose_signal : 'value signal -> unit

(** [map f sig] derives a signal publishing [f] applied to each new value of
    [sig].
    Raises [Invalid_argument] when [sig] is already disposed. *)
val map : ('left -> 'output) -> 'left signal -> 'output signal

(** [map2 f left right] derives a signal publishing [f] applied to the latest
    values of both inputs. Both signals must share one scheduler.
    Raises [Invalid_argument] when the schedulers differ or either input is
    disposed. *)
val map2 : ('left -> 'right -> 'output) -> 'left signal -> 'right signal -> 'output signal

(** [cutoff equal sig] derives a signal that republishes only when [equal
    old new] is [false]. *)
val cutoff : ('value -> 'value -> bool) -> 'value signal -> 'value signal

(** {1 Scope operations} *)

(** [scope name] creates an unmounted root scope. *)
val scope : string -> scope

(** [child_scope name parent] creates an unmounted scope owned by [parent];
    disposing [parent] disposes it. *)
val child_scope : string -> scope -> scope

(** [make_scope name] is {!val-scope}; kept for API parity. *)
val make_scope : string -> scope

(** [on_mount sc f] runs [f] when [sc] mounts; if already mounted, runs [f]
    immediately. *)
val on_mount : scope -> (unit -> unit) -> unit

(** [on_unmount sc f] registers [f] to run when [sc] is disposed. *)
val on_unmount : scope -> (unit -> unit) -> unit

(** [on_dispose sc f] registers [f] to run when [sc] is disposed; runs
    immediately on an already-disposed scope. *)
val on_dispose : scope -> (unit -> unit) -> unit

(** [register_cleanup sc f] registers [f] and returns a {!subscription} that
    cancels it. *)
val register_cleanup : scope -> (unit -> unit) -> subscription

(** [own sc sub] ties [sub]'s lifetime to [sc]; on a disposed scope [sub] is
    cancelled immediately. *)
val own : scope -> subscription -> subscription

(** [own_signal sc sig] ties [sig]'s lifetime to [sc]. *)
val own_signal : scope -> 'value signal -> 'value signal

(** [mount sc] marks the scope mounted and runs pending mount callbacks. *)
val mount : scope -> unit

(** [dispose_scope sc] runs cleanups and unmount callbacks, cancels owned
    subscriptions, and disposes child scopes; idempotent. *)
val dispose_scope : scope -> unit

(** [active sc] is [true] while [sc] is mounted and not disposed. *)
val active : scope -> bool

(** [state_slot name] creates a slot template for {!state_at}. *)
val state_slot : string -> 'value state_slot

(** [state_at owner sc slot initial] returns the per-scope state for [slot] in
    [sc], creating it with [initial] on first use; the entry is removed when
    [sc] is disposed.
    Raises [Invalid_argument] on a disposed scope. *)
val state_at : scheduler -> scope -> 'value state_slot -> 'value -> 'value state

(** {1 Switch operations} *)

(** [switch parent key_sig equal mount] mounts [mount key] in a child scope of
    [parent] for the current key, and on each published change disposes the
    previous scope and mounts a new one when [equal] reports the keys differ. *)
val switch : scope -> 'key signal -> ('key -> 'key -> bool) -> ('key -> scope) -> 'key switch

(** [dispose_switch sw] disposes the switch and its current child scope;
    idempotent. *)
val dispose_switch : 'key switch -> unit

(** {1 Keyed collection operations} *)

(** [find_entry_index entries key compare] is the index of the entry whose key
    compares [0] under [compare], or [None]. *)
val find_entry_index : ('key, 'item) keyed_entry list -> 'key -> ('key -> 'key -> int) -> int option

(** [key_index items key_of compare] builds a key-to-index lookup for [items].
    Raises [Invalid_argument] on duplicate keys. *)
val key_index : 'item list -> ('item -> 'key) -> ('key -> 'key -> int) -> 'key -> int option

(** [remove_entry_at entries i] drops the entry at index [i]. *)
val remove_entry_at : ('key, 'item) keyed_entry list -> int -> ('key, 'item) keyed_entry list

(** [insert_entry_at entries i entry] inserts [entry] at index [i],
    clamping [i] into the list bounds. *)
val insert_entry_at : ('key, 'item) keyed_entry list -> int -> ('key, 'item) keyed_entry -> ('key, 'item) keyed_entry list

(** [move_entry entries from_i to_i] relocates the entry at [from_i] to
    [to_i], clamping both into the list bounds. *)
val move_entry : ('key, 'item) keyed_entry list -> int -> int -> ('key, 'item) keyed_entry list

(** [reconcile_keyed owner entries_ref items key_of compare mount on_patch]
    diffs [items] against [entries_ref]: entries whose keys disappeared are
    removed (scope disposed, [Remove] emitted, greatest index first), kept
    entries are repositioned ([Move] emitted on index change), and new keys
    are mounted and inserted ([Insert] emitted). *)
val reconcile_keyed : scheduler -> ('key, 'item) keyed_entry list ref -> 'item list -> ('item -> 'key) -> ('key -> 'key -> int) -> ('item signal -> scope) -> ('key keyed_patch -> unit) -> unit

(** [keyed parent items_sig key_of compare mount on_patch] subscribes to
    [items_sig] and reconciles each published list into per-key entries, each
    backed by a state and mounted scope, emitting {!keyed_patch} values to
    [on_patch]. *)
val keyed : scope -> 'item list signal -> ('item -> 'key) -> ('key -> 'key -> int) -> ('item signal -> scope) -> ('key keyed_patch -> unit) -> ('key, 'item) keyed

(** [keyed_find_scope k key] is the scope of the entry with [key].
    Raises [Invalid_argument] when no entry carries [key]. *)
val keyed_find_scope : ('key, 'item) keyed -> 'key -> scope

(** [dispose_keyed k] cancels the reconciliation subscription and disposes all
    entry scopes; idempotent. *)
val dispose_keyed : ('key, 'item) keyed -> unit
