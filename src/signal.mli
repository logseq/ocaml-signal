(** A small statically typed incremental runtime, ported from signal-lg.

    Reactive values, scoped state, lifecycle, effects, switches, and keyed
    collections. Each ['value signal] stores only callbacks accepting that same
    ['value] type; the scheduler stores only zero-argument tasks. *)

type stabilization_diagnostics = {
  stabilization_generation : int;
  stabilization_rounds : int;
  stabilization_effects : int;
  stabilization_dirty_tasks : int;
}

type scheduler = {
  effects : (unit -> unit) list ref;
  dirty : (unit -> unit) list ref;
  generation_value : int ref;
  last_stabilization_value : stabilization_diagnostics ref;
}

type subscription = { disposed : bool ref; cancel : unit -> unit }

type 'value subscriber = { subscriber_id : int; callback : 'value -> unit }

type cleanup_entry = { cleanup_id : int; cleanup_callback : unit -> unit }

type 'value signal = {
  owner : scheduler;
  current : 'value ref;
  next_subscriber_id : int ref;
  subscribers : 'value subscriber list ref;
  upstream_subscriptions : subscription list ref;
  disposed_signal : bool ref;
}

type 'value state = {
  state_signal : 'value signal;
  pending : 'value option ref;
  scheduled : bool ref;
}

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

type 'value state_slot = {
  slot_name : string;
  slot_states : (int, 'value state) Hashtbl.t;
}

type 'key switch = {
  switch_subscription : subscription;
  switch_scope : scope ref;
  switch_disposed : bool ref;
}

type 'key keyed_patch =
  | Insert of 'key * int
  | Remove of 'key * int
  | Move of 'key * int * int

type ('key, 'item) keyed_entry = {
  entry_key : 'key;
  entry_state : 'item state;
  entry_scope : scope;
}

type ('key, 'item) keyed = {
  keyed_subscription : subscription;
  keyed_entries : ('key, 'item) keyed_entry list ref;
  keyed_compare : 'key -> 'key -> int;
  keyed_disposed : bool ref;
}

val scheduler : unit -> scheduler
val generation : scheduler -> int
val last_stabilization : scheduler -> stabilization_diagnostics
val enqueue_effect : scheduler -> (unit -> unit) -> unit
val enqueue_dirty : scheduler -> (unit -> unit) -> unit
val schedule_once : scheduler -> bool ref -> (unit -> unit) -> unit
val stabilize : scheduler -> unit
val constant : scheduler -> 'value -> 'value signal
val state : scheduler -> 'value -> 'value state
val value : 'value state -> 'value signal
val sample : 'value signal -> 'value
val get : 'value state -> 'value
val set : 'value state -> 'value -> unit
val update : 'value state -> ('value -> 'value) -> unit
val subscribe : ?emit_initial:bool -> 'value signal -> ('value -> unit) -> subscription
val observe : 'value signal -> ('value -> unit) -> subscription
val dispose_subscription : subscription -> unit
val dispose_signal : 'value signal -> unit
val map : ('left -> 'output) -> 'left signal -> 'output signal
val map2 : ('left -> 'right -> 'output) -> 'left signal -> 'right signal -> 'output signal
val cutoff : ('value -> 'value -> bool) -> 'value signal -> 'value signal
val scope : string -> scope
val child_scope : string -> scope -> scope
val make_scope : string -> scope
val on_mount : scope -> (unit -> unit) -> unit
val on_unmount : scope -> (unit -> unit) -> unit
val on_dispose : scope -> (unit -> unit) -> unit
val register_cleanup : scope -> (unit -> unit) -> subscription
val own : scope -> subscription -> subscription
val own_signal : scope -> 'value signal -> 'value signal
val mount : scope -> unit
val dispose_scope : scope -> unit
val active : scope -> bool
val state_slot : string -> 'value state_slot
val state_at : scheduler -> scope -> 'value state_slot -> 'value -> 'value state
val switch : scope -> 'key signal -> ('key -> 'key -> bool) -> ('key -> scope) -> 'key switch
val dispose_switch : 'key switch -> unit
val find_entry_index : ('key, 'item) keyed_entry list -> 'key -> ('key -> 'key -> int) -> int option
val key_index : 'item list -> ('item -> 'key) -> ('key -> 'key -> int) -> 'key -> int option
val remove_entry_at : ('key, 'item) keyed_entry list -> int -> ('key, 'item) keyed_entry list
val insert_entry_at : ('key, 'item) keyed_entry list -> int -> ('key, 'item) keyed_entry -> ('key, 'item) keyed_entry list
val move_entry : ('key, 'item) keyed_entry list -> int -> int -> ('key, 'item) keyed_entry list
val reconcile_keyed : scheduler -> ('key, 'item) keyed_entry list ref -> 'item list -> ('item -> 'key) -> ('key -> 'key -> int) -> ('item signal -> scope) -> ('key keyed_patch -> unit) -> unit
val keyed : scope -> 'item list signal -> ('item -> 'key) -> ('key -> 'key -> int) -> ('item signal -> scope) -> ('key keyed_patch -> unit) -> ('key, 'item) keyed
val keyed_find_scope : ('key, 'item) keyed -> 'key -> scope
val dispose_keyed : ('key, 'item) keyed -> unit
