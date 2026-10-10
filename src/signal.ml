type stabilization_diagnostics = {
  stabilization_generation : int;
  stabilization_rounds : int;
  stabilization_effects : int;
  stabilization_dirty_tasks : int;
}

module Rank_map = Map.Make (Int)

type computation_queue = (unit -> unit) Queue.t Rank_map.t

type scheduler = {
  effects : (unit -> unit) Queue.t;
  dirty : (unit -> unit) Queue.t;
  computations : computation_queue ref;
  observers : (unit -> unit) Queue.t;
  generation_value : int ref;
  last_stabilization_value : stabilization_diagnostics ref;
}

type subscription = { disposed : bool ref; cancel : unit -> unit }

type 'value subscriber = {
  mutable callback : 'value -> unit;
  subscriber_disposed : bool ref;
  subscriber_observer : bool;
  mutable previous : 'value subscriber option;
  mutable next : 'value subscriber option;
}

type 'value subscribers = {
  mutable first : 'value subscriber option;
  mutable last : 'value subscriber option;
}

type cleanup_entry = {
  cleanup_live : bool ref;
  cleanup_callback : unit -> unit;
}

type 'value signal = {
  owner : scheduler;
  rank : int;
  current : 'value ref;
  subscribers : 'value subscribers;
  upstream_subscriptions : subscription list ref;
  live_subscribers : int ref;
  reconnect : (unit -> unit) option ref;
  disposed_signal : bool ref;
}

type 'value state = {
  state_signal : 'value signal;
  pending : 'value option ref;
  scheduled : bool ref;
}

(* Bounded deferred-removal accounting: [live_cleanups] counts registered
   callbacks, [cleanup_dead] counts cancelled list nodes awaiting compaction,
   [owned_slots] is the owned list length, [owned_dead] counts cancellations
   reported through owned handles, and [owned_scan] is the list length that
   triggers a sweep for cancellations reported through raw handles. *)
type scope_registry = {
  mutable live_cleanups : int;
  mutable cleanup_dead : int;
  mutable owned_slots : int;
  mutable owned_dead : int;
  mutable owned_scan : int;
}

let make_scope_registry () =
  {
    live_cleanups = 0;
    cleanup_dead = 0;
    owned_slots = 0;
    owned_dead = 0;
    owned_scan = 64;
  }

type scope = {
  scope_id : int;
  scope_name : string;
  cleanup_callbacks : cleanup_entry list ref;
  mount_callbacks : (unit -> unit) list ref;
  unmount_callbacks : (unit -> unit) list ref;
  owned_subscriptions : subscription list ref;
  registry : scope_registry;
  mounted : bool ref;
  disposed_scope : bool ref;
}

type 'value state_slot = {
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

let scheduler () =
  {
    effects = Queue.create ();
    dirty = Queue.create ();
    computations = ref Rank_map.empty;
    observers = Queue.create ();
    generation_value = ref 0;
    last_stabilization_value =
      ref
        {
          stabilization_generation = 0;
          stabilization_rounds = 0;
          stabilization_effects = 0;
          stabilization_dirty_tasks = 0;
        };
  }

let generation owner = !(owner.generation_value)

let last_stabilization owner = !(owner.last_stabilization_value)

let enqueue_effect owner f = Queue.add f owner.effects
let enqueue_dirty owner task = Queue.add task owner.dirty

let schedule_once owner scheduled task =
  if !scheduled
  then ()
  else begin
    scheduled := true;
    enqueue_dirty owner (fun () ->
        scheduled := false;
        task ())
  end

let rec schedule_computation reactive scheduled task =
  if not !scheduled then begin
    scheduled := true;
    let queue =
      match Rank_map.find_opt reactive.rank !(reactive.owner.computations) with
      | Some queue -> queue
      | None ->
          let queue = Queue.create () in
          reactive.owner.computations :=
            Rank_map.add reactive.rank queue !(reactive.owner.computations);
          queue
    in
    Queue.add
      (fun () ->
        scheduled := false;
        if
          not !(reactive.disposed_signal)
          && !(reactive.upstream_subscriptions) <> []
        then
          try task ()
          with exn ->
            (* Keep the node stale: requeue the failed computation so the next
               stabilize recomputes it, then propagate the failure. *)
            schedule_computation reactive scheduled task;
            raise exn)
      queue
  end

let run_tasks queue count run =
  (* Taking only the snapshot length leaves new work for the next round and
     keeps unexecuted tasks queued if a callback raises. A nested stabilize
     may have drained the queue first, so stop at empty instead of raising
     Queue.Empty. *)
  let remaining = ref count in
  while !remaining > 0 && not (Queue.is_empty queue) do
    decr remaining;
    run (Queue.take queue)
  done

let prune_computation_queue owner rank queue =
  (* A nested stabilize may already have drained and pruned this rank and new
     work may have rebuilt it; only remove the entry while the map still holds
     this exact queue. *)
  if Queue.is_empty queue then
    match Rank_map.find_opt rank !(owner.computations) with
    | Some current when current == queue ->
        owner.computations := Rank_map.remove rank !(owner.computations)
    | _ -> ()

let max_stabilization_rounds = 10000

exception Stabilization_limit_exceeded of int * int * int

let stabilize_impl owner =
  let worked = ref false in
  let rounds = ref 0 in
  let effect_count = ref 0 in
  let dirty_count = ref 0 in
  let continue = ref true in
  while !continue do
    let effects = Queue.length owner.effects in
    let dirty = Queue.length owner.dirty in
    if
      effects = 0 && dirty = 0
      && Rank_map.is_empty !(owner.computations)
      && Queue.is_empty owner.observers
    then continue := false
    else begin
      worked := true;
      incr rounds;
      if !rounds > max_stabilization_rounds then
        raise
          (Stabilization_limit_exceeded
             (max_stabilization_rounds, !effect_count, !dirty_count));
      run_tasks owner.effects effects (fun f ->
          incr effect_count;
          f ());
      let pending_dirty = Queue.length owner.dirty in
      run_tasks owner.dirty pending_dirty (fun task ->
          incr dirty_count;
          task ());
      if effects = 0 && pending_dirty = 0 then begin
        (* Mutations settled: recompute the whole graph in dependency order —
           every rank in one pass so the round cap measures real divergence,
           not graph depth — then flush observer notifications. *)
        while not (Rank_map.is_empty !(owner.computations)) do
          let rank, queue = Rank_map.min_binding !(owner.computations) in
          (try
            run_tasks queue (Queue.length queue) (fun task ->
                incr dirty_count;
                task ())
          with exn ->
            prune_computation_queue owner rank queue;
            raise exn);
          prune_computation_queue owner rank queue
        done;
        (* Observers read a consistent graph: all queued computations finished
           first. A failing observer does not starve the rest; the first error
           propagates once the snapshot drains. Work observers stage runs in a
           later round. *)
        let first_error = ref None in
        run_tasks owner.observers (Queue.length owner.observers) (fun f ->
            try f ()
            with exn ->
              if !first_error = None then first_error := Some exn);
        match !first_error with None -> () | Some exn -> raise exn
      end
    end
  done;
  if !worked then incr owner.generation_value;
  owner.last_stabilization_value :=
    {
      stabilization_generation = !(owner.generation_value);
      stabilization_rounds = !rounds;
      stabilization_effects = !effect_count;
      stabilization_dirty_tasks = !dirty_count;
    }

(* Fun.protect's exceptional path restores raw backtraces, which Melange does
   not implement. These finalizers only reset internal refs and cannot raise. *)
let with_finally finally f =
  match f () with
  | result -> finally (); result
  | exception exn -> finally (); raise exn

(* A nested stabilize on the same scheduler drains queued work synchronously:
   callers inside tasks and observers may flush pending writes before reading
   derived state. Snapshot-bounded task runs and identity-checked pruning keep
   the outer fixpoint loop correct across reentrant drains. *)
let stabilize owner = stabilize_impl owner

let constant owner initial =
  {
    owner;
    rank = 0;
    current = ref initial;
    subscribers = { first = None; last = None };
    upstream_subscriptions = ref [];
    live_subscribers = ref 0;
    reconnect = ref None;
    disposed_signal = ref false;
  }

(* With no live subscribers a derived node has no downstream reader; releasing
   its upstream subscriptions stops recomputation and lets unneeded sources
   deactivate too. The next subscription resubscribes through {!reconnect}. *)
let release_upstream_if_orphaned reactive =
  if
    !(reactive.live_subscribers) = 0
    && !(reactive.upstream_subscriptions) <> []
  then begin
    let subscriptions = !(reactive.upstream_subscriptions) in
    reactive.upstream_subscriptions := [];
    List.iter (fun subscription -> subscription.cancel ()) subscriptions
  end

(* Sampling an orphaned derived rescues it for the read only: reconnect
   recomputes the fresh value, then release drops the upstream subscriptions
   again so a read without subscribers does not leak liveness. *)
let sample reactive =
  if
    (not !(reactive.disposed_signal))
    && !(reactive.live_subscribers) = 0
    && !(reactive.upstream_subscriptions) = []
  then
    match !(reactive.reconnect) with
    | Some reconnect ->
        reconnect ();
        release_upstream_if_orphaned reactive
    | None -> ()
  else ();
  !(reactive.current)

let get = sample

let signal_is_disposed reactive = !(reactive.disposed_signal)

let subscribe_gen observer emit_initial reactive callback =
  if !(reactive.disposed_signal) then
    invalid_arg "cannot observe a disposed signal";
  (* Reactivate before linking: the reconnection recompute notifies only the
     subscribers that were already live, and the emit-initial call below sees
     the fresh value. *)
  (match !(reactive.reconnect) with
  | Some reconnect when !(reactive.upstream_subscriptions) = [] ->
      reconnect ()
  | _ -> ());
  let disposed = ref false in
  let subscriber_value =
    {
      callback;
      subscriber_disposed = disposed;
      subscriber_observer = observer;
      previous = reactive.subscribers.last;
      next = None;
    }
  in
  let cancel () =
    if !disposed then ()
    else begin
      disposed := true;
      (match subscriber_value.previous with
      | None -> reactive.subscribers.first <- subscriber_value.next
      | Some previous -> previous.next <- subscriber_value.next);
      (match subscriber_value.next with
      | None -> reactive.subscribers.last <- subscriber_value.previous
      | Some next -> next.previous <- subscriber_value.previous);
      subscriber_value.previous <- None;
      subscriber_value.next <- None;
      subscriber_value.callback <- ignore;
      reactive.live_subscribers := !(reactive.live_subscribers) - 1;
      release_upstream_if_orphaned reactive
    end
  in
  (match reactive.subscribers.last with
  | None -> reactive.subscribers.first <- Some subscriber_value
  | Some previous -> previous.next <- Some subscriber_value);
  reactive.subscribers.last <- Some subscriber_value;
  reactive.live_subscribers := !(reactive.live_subscribers) + 1;
  if emit_initial then
    (try callback (sample reactive) with exn -> cancel (); raise exn);
  { disposed; cancel }

let subscribe ?(emit_initial = true) reactive callback =
  subscribe_gen true emit_initial reactive callback

(* Computation subscribers only enqueue ranked tasks; they run inline during
   publication so the whole graph settles before observer callbacks. *)
let subscribe_computation reactive callback =
  subscribe_gen false false reactive callback

let observe reactive callback = subscribe ~emit_initial:true reactive callback

let dispose_subscription subscription = subscription.cancel ()

let subscription_disposed subscription = !(subscription.disposed)

(* External cancel handles share the same record shape so {!dispose_subscription}
   and {!subscription_disposed} work uniformly; cancellation is idempotent. *)
let make_subscription cancel =
  let disposed = ref false in
  {
    disposed;
    cancel =
      (fun () ->
        if not !disposed then begin
          disposed := true;
          cancel ()
        end);
  }

let dispose_signal reactive =
  if !(reactive.disposed_signal) then ()
  else begin
    reactive.disposed_signal := true;
    List.iter dispose_subscription !(reactive.upstream_subscriptions);
    reactive.upstream_subscriptions := [];
    let rec clear = function
      | None -> ()
      | Some subscriber ->
          let next = subscriber.next in
          subscriber.subscriber_disposed := true;
          subscriber.callback <- ignore;
          subscriber.previous <- None;
          subscriber.next <- None;
          clear next
    in
    clear reactive.subscribers.first;
    reactive.subscribers.first <- None;
    reactive.subscribers.last <- None;
    reactive.live_subscribers := 0
  end

let run_each f values =
  let first_error = ref None in
  List.iter
    (fun value ->
      try f value
      with exn -> if !first_error = None then first_error := Some exn)
    values;
  match !first_error with None -> () | Some exn -> raise exn

let run_callbacks callbacks = run_each (fun callback -> callback ()) callbacks

let publish reactive next_value =
  if not !(reactive.disposed_signal) then begin
    reactive.current := next_value;
    let rec snapshot accumulated = function
      | None -> List.rev accumulated
      | Some subscriber -> snapshot (subscriber :: accumulated) subscriber.next
    in
    (* Computation subscribers schedule ranked work inline; user, switch, and
       keyed observers queue onto the observer queue drained once the whole
       computation phase completes, so observers read a consistent graph.
       Cancelled subscribers neutralize their callback; the thunk rechecks
       disposal at run time. *)
    run_each
      (fun subscriber_value ->
        if not !(reactive.disposed_signal) then
          if subscriber_value.subscriber_observer then
            Queue.add
              (fun () ->
                if not !(reactive.disposed_signal) then
                  subscriber_value.callback next_value)
              reactive.owner.observers
          else subscriber_value.callback next_value)
      (snapshot [] reactive.subscribers.first)
  end

let state owner initial =
  {
    state_signal = constant owner initial;
    pending = ref None;
    scheduled = ref false;
  }

let value state_value = state_value.state_signal

let get_state state_value = sample (value state_value)

let set state_value next_value =
  state_value.pending := Some next_value;
  schedule_once (value state_value).owner state_value.scheduled (fun () ->
      match !(state_value.pending) with
      | Some pending_value ->
        state_value.pending := None;
        publish (value state_value) pending_value
      | None -> ())

let update state_value update_fn =
  let current =
    match !(state_value.pending) with
    | Some pending_value -> pending_value
    | None -> get_state state_value
  in
  set state_value (update_fn current)

(* The recompute task of a derived that loses its last subscriber is skipped
   while its upstream subscriptions are released; resubscribing recomputes on
   demand through {!reconnect}. *)
let map transform source =
  let derived =
    {
      (constant source.owner (transform (sample source))) with
      rank = source.rank + 1;
    }
  in
  let scheduled = ref false in
  let subscribe_upstream () =
    subscribe_computation source (fun _current ->
        schedule_computation derived scheduled (fun () ->
            publish derived (transform (sample source))))
  in
  derived.upstream_subscriptions := [ subscribe_upstream () ];
  derived.reconnect :=
    Some
      (fun () ->
        derived.upstream_subscriptions := [ subscribe_upstream () ];
        let next_value = transform (sample source) in
        if next_value != !(derived.current) then publish derived next_value);
  derived

let map2 transform left right =
  if left.owner != right.owner then
    invalid_arg "map inputs must share one scheduler";
  if !(left.disposed_signal) || !(right.disposed_signal) then
    invalid_arg "cannot derive from a disposed signal";
  let derived =
    {
      (constant left.owner (transform (sample left) (sample right))) with
      rank = max left.rank right.rank + 1;
    }
  in
  let scheduled = ref false in
  let recompute _changed =
    schedule_computation derived scheduled (fun () ->
        publish derived (transform (sample left) (sample right)))
  in
  let subscribe_upstream () =
    let left_subscription = subscribe_computation left recompute in
    try
      let right_subscription = subscribe_computation right recompute in
      [ left_subscription; right_subscription ]
    with exn -> dispose_subscription left_subscription; raise exn
  in
  derived.upstream_subscriptions := subscribe_upstream ();
  derived.reconnect :=
    Some
      (fun () ->
        derived.upstream_subscriptions := subscribe_upstream ();
        let next_value = transform (sample left) (sample right) in
        if next_value != !(derived.current) then publish derived next_value);
  derived

let cutoff equal source =
  let derived =
    { (constant source.owner (sample source)) with rank = source.rank + 1 }
  in
  let scheduled = ref false in
  let subscribe_upstream () =
    subscribe_computation source (fun _ ->
        schedule_computation derived scheduled (fun () ->
            let next_value = sample source in
            if not (equal (sample derived) next_value) then
              publish derived next_value))
  in
  derived.upstream_subscriptions := [ subscribe_upstream () ];
  derived.reconnect :=
    Some
      (fun () ->
        derived.upstream_subscriptions := [ subscribe_upstream () ];
        let next_value = sample source in
        if not (equal (sample derived) next_value) then
          publish derived next_value);
  derived

let next_scope_id = ref 0

let make_scope name =
  incr next_scope_id;
  {
    scope_id = !next_scope_id;
    scope_name = name;
    cleanup_callbacks = ref [];
    mount_callbacks = ref [];
    unmount_callbacks = ref [];
    owned_subscriptions = ref [];
    registry = make_scope_registry ();
    mounted = ref false;
    disposed_scope = ref false;
  }

(* Amortized compaction: once dead nodes outnumber live ones, one filtered
   pass restores the bound — at most twice as many stored nodes as live
   registrations. Skipped while the scope disposes; its lists are cleared. *)
let compact_cleanups_if_unbalanced scope_value =
  let registry = scope_value.registry in
  if
    not !(scope_value.disposed_scope)
    && registry.cleanup_dead > registry.live_cleanups
  then begin
    scope_value.cleanup_callbacks :=
      List.filter
        (fun entry -> not !(entry.cleanup_live))
        !(scope_value.cleanup_callbacks);
    registry.cleanup_dead <- 0
  end

let register_cleanup scope_value callback =
  if !(scope_value.disposed_scope) then begin
    callback ();
    { disposed = ref true; cancel = (fun () -> ()) }
  end
  else begin
    let disposed = ref false in
    let action = ref (Some callback) in
    let registry = scope_value.registry in
    let mark_dead () =
      if not !(scope_value.disposed_scope) then begin
        registry.live_cleanups <- registry.live_cleanups - 1;
        registry.cleanup_dead <- registry.cleanup_dead + 1;
        compact_cleanups_if_unbalanced scope_value
      end
    in
    let entry =
      {
        cleanup_live = disposed;
        cleanup_callback =
          (fun () ->
            match !action with
            | None -> ()
            | Some callback ->
                action := None;
                disposed := true;
                mark_dead ();
                callback ());
      }
    in
    let cancel () =
      if !disposed then ()
      else begin
        disposed := true;
        (* Releasing the captured callback before counting keeps a cancelled
           entry from retaining the user's resources. *)
        action := None;
        mark_dead ()
      end
    in
    registry.live_cleanups <- registry.live_cleanups + 1;
    scope_value.cleanup_callbacks := entry :: !(scope_value.cleanup_callbacks);
    { disposed; cancel }
  end

let run_cleanups = run_callbacks

let rec child_scope name parent =
  if !(parent.disposed_scope)
  then invalid_arg "cannot create a child of a disposed scope";
  let child = make_scope name in
  ignore
    (own child (register_cleanup parent (fun () -> dispose_scope child)));
  child

(* Dead owned nodes are dropped when counted cancellations exceed half the
   list; cancellations through a raw handle are swept when the list grows past
   the scan watermark. Both keep the retained list within a constant factor
   of live handles. *)
and sweep_owned scope_value =
  let registry = scope_value.registry in
  scope_value.owned_subscriptions :=
    List.filter
      (fun current -> not !(current.disposed))
      !(scope_value.owned_subscriptions);
  registry.owned_slots <- List.length !(scope_value.owned_subscriptions);
  registry.owned_dead <- 0;
  registry.owned_scan <- max 64 (2 * registry.owned_slots)

and own scope_value subscription =
  if !(scope_value.disposed_scope) then begin
    dispose_subscription subscription;
    subscription
  end
  else begin
    let registry = scope_value.registry in
    registry.owned_slots <- registry.owned_slots + 1;
    let owned =
      {
        disposed = subscription.disposed;
        cancel =
          (fun () ->
            if !(subscription.disposed) then ()
            else begin
              subscription.cancel ();
              if not !(scope_value.disposed_scope) then begin
                registry.owned_dead <- registry.owned_dead + 1;
                if registry.owned_dead * 2 > registry.owned_slots then
                  sweep_owned scope_value
              end
            end);
      }
    in
    scope_value.owned_subscriptions :=
      owned :: !(scope_value.owned_subscriptions);
    (if registry.owned_slots >= registry.owned_scan then
      sweep_owned scope_value);
    owned
  end


and dispose_scope scope_value =
  if !(scope_value.disposed_scope) then ()
  else begin
    scope_value.disposed_scope := true;
    let cleanups = List.rev !(scope_value.cleanup_callbacks) in
    let subscriptions = List.rev !(scope_value.owned_subscriptions) in
    let unmounts =
      if !(scope_value.mounted) then List.rev !(scope_value.unmount_callbacks)
      else []
    in
    scope_value.mounted := false;
    scope_value.cleanup_callbacks := [];
    scope_value.owned_subscriptions := [];
    scope_value.mount_callbacks := [];
    scope_value.unmount_callbacks := [];
    let registry = scope_value.registry in
    registry.live_cleanups <- 0;
    registry.cleanup_dead <- 0;
    registry.owned_slots <- 0;
    registry.owned_dead <- 0;
    registry.owned_scan <- 64;
    run_cleanups
      [
        (fun () ->
          run_cleanups
            (List.map (fun cleanup -> cleanup.cleanup_callback) cleanups));
        (fun () ->
          run_cleanups
            (List.map
               (fun subscription -> fun () -> dispose_subscription subscription)
               subscriptions));
        (fun () -> run_cleanups unmounts);
      ]
  end

let scope name = make_scope name

let scope_id scope_value = scope_value.scope_id
let scope_name scope_value = scope_value.scope_name
let scope_is_disposed scope_value = !(scope_value.disposed_scope)
let scope_cleanup_entries scope_value = !(scope_value.cleanup_callbacks)
let scope_owned_entries scope_value = !(scope_value.owned_subscriptions)
let cleanup_entry_run entry = entry.cleanup_callback ()

let scope_cleanup_count scope_value = scope_value.registry.live_cleanups

let scope_owned_count scope_value =
  List.fold_left
    (fun count subscription -> if !(subscription.disposed) then count else count + 1)
    0 !(scope_value.owned_subscriptions)

let on_mount scope_value callback =
  if !(scope_value.disposed_scope) then ()
  else if !(scope_value.mounted) then callback ()
  else scope_value.mount_callbacks := callback :: !(scope_value.mount_callbacks)

let on_unmount scope_value callback =
  if not !(scope_value.disposed_scope) then
    scope_value.unmount_callbacks :=
      callback :: !(scope_value.unmount_callbacks)

let on_dispose scope_value callback =
  ignore (register_cleanup scope_value callback)

let own_signal scope_value reactive =
  on_dispose scope_value (fun () -> dispose_signal reactive);
  reactive

let mount scope_value =
  if !(scope_value.disposed_scope) || !(scope_value.mounted) then ()
  else begin
    scope_value.mounted := true;
    let callbacks = List.rev !(scope_value.mount_callbacks) in
    scope_value.mount_callbacks := [];
    List.iter
      (fun callback -> if not !(scope_value.disposed_scope) then callback ())
      callbacks
  end

let active scope_value =
  !(scope_value.mounted) && not !(scope_value.disposed_scope)

let state_slot _name = { slot_states = Hashtbl.create 8 }

let state_slot_count slot = Hashtbl.length slot.slot_states

let state_at scheduler scope_value slot initial =
  if !(scope_value.disposed_scope)
  then invalid_arg "cannot create state in a disposed scope";
  let scope_id = scope_value.scope_id in
  match Hashtbl.find_opt slot.slot_states scope_id with
  | Some existing -> existing
  | None ->
    let created = state scheduler initial in
    Hashtbl.replace slot.slot_states scope_id created;
    on_dispose scope_value (fun () ->
        dispose_signal (value created);
        Hashtbl.remove slot.slot_states scope_id);
    created

let dispose_switch switch_value =
  if !(switch_value.switch_disposed)
  then ()
  else begin
    switch_value.switch_disposed := true;
    run_cleanups
      [ (fun () -> dispose_subscription switch_value.switch_subscription);
        (fun () -> dispose_scope !(switch_value.switch_scope)) ]
  end

(* Collection handles unregister themselves on manual disposal through the
   counted owned handle. Clearing the action also prevents an externally
   retained disposed handle retaining its owner and former resources. *)
let own_lifetime parent cleanup =
  let action = ref (fun () -> ()) in
  let disposed = ref false in
  let handle = { disposed; cancel = (fun () -> !action ()) } in
  action := (fun () ->
    action := (fun () -> ());
    disposed := true;
    cleanup ());
  let owned = own parent handle in
  fun () -> owned.cancel ()

let switch parent source equal mount_scope =
  if !(parent.disposed_scope) then
    invalid_arg "cannot switch in a disposed scope";
  if !(source.disposed_signal) then
    invalid_arg "cannot switch from a disposed signal";
  let initial_key = sample source in
  let current_key = ref initial_key in
  (* Install ownership and observation before calling user factories. The
     provisional scope gives disposal a valid target during construction. *)
  let current_scope = ref (scope "switch:initializing") in
  let disposed = ref false in
  let cancelled = ref (fun () -> ()) in
  let pending = ref None in
  let busy = ref true in
  let replace next_key =
    if !disposed ||
       (not !(!current_scope.disposed_scope) && equal !current_key next_key) then ()
    else begin
      dispose_scope !current_scope;
      if not !disposed then begin
        let next_scope = mount_scope next_key in
        if !disposed then dispose_scope next_scope
        else begin
          current_key := next_key;
          current_scope := next_scope;
          (try mount next_scope with exn ->
            (try dispose_scope next_scope with _ -> ());
            raise exn)
        end
      end
    end
  in
  let drain () =
    busy := true;
    with_finally (fun () -> busy := false) (fun () ->
      while !pending <> None && not !disposed do
        match !pending with
        | None -> ()
        | Some next_key -> pending := None; replace next_key
      done)
  in
  let observer = subscribe ~emit_initial:false source (fun next_key ->
    if not !disposed then begin
      pending := Some next_key;
      if not !busy then drain ()
    end) in
  let subscription = { observer with cancel = (fun () ->
    pending := None;
    run_cleanups [observer.cancel; !cancelled]) } in
  let switch_value = {
    switch_subscription = subscription;
    switch_scope = current_scope;
    switch_disposed = disposed;
  } in
  try
    cancelled := own_lifetime parent (fun () -> dispose_switch switch_value);
    let initial_scope = mount_scope initial_key in
    dispose_scope !current_scope;
    current_scope := initial_scope;
    if !disposed then dispose_scope initial_scope else mount initial_scope;
    drain ();
    switch_value
  with exn ->
    let original = exn in
    (try run_cleanups [!cancelled; (fun () -> dispose_scope !current_scope)] with _ -> ());
    raise original

let rec find_entry_index entries key compare index =
  match entries with
  | [] -> None
  | entry :: rest ->
    if compare entry.entry_key key = 0
    then Some index
    else find_entry_index rest key compare (index + 1)

let find_entry_index entries key compare = find_entry_index entries key compare 0

let key_index (type key) (items : 'item list) (key_fn : 'item -> key)
    (compare : key -> key -> int) : key -> int option =
  let module Key_map =
    Map.Make (struct
      type t = key
      let compare = compare
    end)
  in
  let items = Array.of_list items in
  let rec loop index indexes =
    if index = Array.length items
    then indexes
    else
      let key = key_fn items.(index) in
      if Key_map.mem key indexes
      then invalid_arg "keyed collection contains a duplicate key"
      else loop (index + 1) (Key_map.add key index indexes)
  in
  let indexes = loop 0 Key_map.empty in
  fun key -> Key_map.find_opt key indexes

let remove_entry_at entries removed_index =
  List.filteri (fun index _entry -> index <> removed_index) entries

let insert_entry_at entries inserted_index inserted =
  let inserted_index = max 0 (min inserted_index (List.length entries)) in
  let rec loop index result remaining =
    match remaining with
    | [] ->
        let result =
          if inserted_index = index then inserted :: result else result
        in
        List.rev result
    | entry :: rest ->
        let result =
          if index = inserted_index then inserted :: result else result
        in
        loop (index + 1) (entry :: result) rest
  in
  loop 0 [] entries

let move_entry entries from_index to_index =
  match entries with
  | [] -> []
  | _ ->
      let from_index = max 0 (min from_index (List.length entries - 1)) in
      let moving = List.nth entries from_index in
      let without = remove_entry_at entries from_index in
      insert_entry_at without to_index moving

let set_keyed_item item_state current =
  let staged =
    match !(item_state.pending) with
    | Some pending -> pending
    | None -> get_state item_state
  in
  if staged != current then set item_state current

let remaining_counts size =
  Array.init (size + 1) (fun index -> index land -index)

let remaining_before counts index =
  let rec loop index total =
    if index = 0 then total
    else loop (index - (index land -index)) (total + counts.(index))
  in
  loop index 0

let remove_remaining counts index =
  let rec loop index =
    if index < Array.length counts then begin
      counts.(index) <- counts.(index) - 1;
      loop (index + (index land -index))
    end
  in
  loop (index + 1)

let reconcile_keyed_unguarded (type key) stopped scheduler
    (entries_ref : (key, 'item) keyed_entry list ref) (items : 'item list)
    (key_fn : 'item -> key) (compare : key -> key -> int) mount_scope on_patch =
  let module Key_map = Map.Make (struct
    type t = key
    let compare = compare
  end) in
  let new_index = key_index items key_fn compare in
  let first_error = ref None in
  let attempt f =
    try Some (f ())
    with exn ->
      if !first_error = None then first_error := Some exn;
      None
  in
  let release entry =
    ignore (attempt (fun () -> dispose_scope entry.entry_scope));
    dispose_signal (value entry.entry_state)
  in
  let patch value =
    if not (stopped ()) then ignore (attempt (fun () -> on_patch value))
  in
  let _, retained, removed =
    List.fold_left
      (fun (index, retained, removed) entry ->
        match new_index entry.entry_key with
        | Some _ -> (index + 1, entry :: retained, removed)
        | None -> (index + 1, retained, (index, entry) :: removed))
      (0, [], []) !entries_ref
  in
  let retained = List.rev retained in
  let size, existing =
    List.fold_left
      (fun (index, entries) entry ->
        (index + 1, Key_map.add entry.entry_key (index, entry) entries))
      (0, Key_map.empty) retained
  in
  let counts = remaining_counts size in
  let created = ref [] in
  let rec loop index result = function
    | [] -> List.rev result
    | _ when stopped () -> List.rev result
    | current :: rest ->
        let key = key_fn current in
        let entry =
          match Key_map.find_opt key existing with
          | Some (original_index, entry) ->
              set_keyed_item entry.entry_state current;
              let before = remaining_before counts original_index in
              if before <> 0 then patch (Move (key, index + before, index));
              remove_remaining counts original_index;
              Some entry
          | None ->
              let item_state = state scheduler current in
              match attempt (fun () -> mount_scope (value item_state)) with
              | None -> dispose_signal (value item_state); None
              | Some child ->
                  let entry = { entry_key = key; entry_state = item_state; entry_scope = child } in
                  created := entry :: !created;
                  ignore (own_signal child (value item_state));
                  if stopped () then (release entry; None)
                  else match attempt (fun () -> mount child) with
                    | None -> release entry; None
                    | Some () ->
                        if stopped () then (release entry; None)
                        else (patch (Insert (key, index)); Some entry)
        in
        match entry with
        | None -> loop index result rest
        | Some entry -> loop (index + 1) (entry :: result) rest
  in
  (* User callback errors do not abandon already mounted scopes or leave removed
     entries in the reusable index. Finish the internal edit, then report the
     first error. External patch consumers must resync after callback failure. *)
  let finish () =
    List.iter
      (fun (index, entry) ->
        patch (Remove (entry.entry_key, index));
        release entry)
      removed;
    let result = loop 0 [] items in
    if stopped () then begin
      List.iter release !created;
      entries_ref := []
    end else entries_ref := result
  in
  (try finish () with exn ->
    let original = match !first_error with None -> exn | Some first -> first in
    List.iter release !created;
    entries_ref := List.filter (fun entry -> not !(entry.entry_scope.disposed_scope)) retained;
    raise original);
  match !first_error with None -> () | Some exn -> raise exn

(* Obj.repr is used only for physical identity across the existential key/item
   types of simultaneous calls; no values are inspected or cast back. *)
let reconciling_entries = ref []

let reconcile_keyed_impl stopped scheduler entries_ref items key_fn compare
    mount_scope on_patch =
  let identity = Obj.repr entries_ref in
  if List.exists (fun running -> running == identity) !reconciling_entries then
    invalid_arg "cannot recursively reconcile the same entries";
  reconciling_entries := identity :: !reconciling_entries;
  with_finally
    (fun () ->
      reconciling_entries :=
        List.filter (fun running -> running != identity) !reconciling_entries)
    (fun () -> reconcile_keyed_unguarded stopped scheduler entries_ref items
      key_fn compare mount_scope on_patch)

let reconcile_keyed scheduler entries_ref items key_fn compare mount_scope on_patch =
  reconcile_keyed_impl (fun () -> false) scheduler entries_ref items key_fn compare
    mount_scope on_patch

let dispose_keyed keyed_value =
  if !(keyed_value.keyed_disposed) then ()
  else begin
    keyed_value.keyed_disposed := true;
    let entries = !(keyed_value.keyed_entries) in
    keyed_value.keyed_entries := [];
    run_cleanups
      ((fun () -> dispose_subscription keyed_value.keyed_subscription)
       :: List.map (fun entry -> fun () -> dispose_scope entry.entry_scope) entries)
  end

let keyed parent source key_fn compare mount_scope on_patch =
  if !(parent.disposed_scope) then
    invalid_arg "cannot create a keyed collection in a disposed scope";
  if !(source.disposed_signal) then
    invalid_arg "cannot reconcile a disposed signal";
  let scheduler = source.owner in
  let entries_ref = ref [] and disposed = ref false in
  let reconciling = ref false and pending = ref None in
  let reconcile items =
    pending := Some items;
    if not !reconciling then begin
      reconciling := true;
      with_finally (fun () -> reconciling := false) (fun () ->
        let first_error = ref None in
        while !pending <> None && not !disposed do
          let items = Option.get !pending in
          pending := None;
          (try reconcile_keyed_impl (fun () -> !disposed) scheduler entries_ref
              items key_fn compare mount_scope on_patch
           with exn -> if !first_error = None then first_error := Some exn)
        done;
        pending := None;
        match !first_error with None -> () | Some exn -> raise exn)
    end
  in
  let subscription = subscribe ~emit_initial:false source reconcile in
  let cancelled = ref (fun () -> ()) in
  let wrapped = { subscription with cancel = (fun () ->
    pending := None;
    run_cleanups [subscription.cancel; !cancelled]) } in
  let keyed_value = { keyed_subscription = wrapped; keyed_entries = entries_ref;
                      keyed_compare = compare; keyed_disposed = disposed } in
  cancelled := own_lifetime parent (fun () -> dispose_keyed keyed_value);
  try
    if not !disposed then reconcile (sample source);
    keyed_value
  with exn ->
    (try dispose_keyed keyed_value with _ -> ());
    raise exn

let keyed_find_scope keyed_value key =
  let rec loop entries =
    match entries with
    | [] -> invalid_arg "keyed scope not found"
    | entry :: rest ->
        if keyed_value.keyed_compare entry.entry_key key = 0 then
          entry.entry_scope
        else loop rest
  in
  loop !(keyed_value.keyed_entries)

let switch_scope switch_value = !(switch_value.switch_scope)
let switch_is_disposed switch_value = !(switch_value.switch_disposed)
let switch_subscription switch_value = switch_value.switch_subscription

let keyed_entries keyed_value = !(keyed_value.keyed_entries)
let keyed_is_disposed keyed_value = !(keyed_value.keyed_disposed)

let make_keyed_entry entry_key entry_state entry_scope =
  { entry_key; entry_state; entry_scope }

let keyed_entry_key entry = entry.entry_key
let keyed_entry_state entry = entry.entry_state
let keyed_entry_scope entry = entry.entry_scope
