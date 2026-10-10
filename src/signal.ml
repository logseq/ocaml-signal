(* Incremental runtime.
   Derived nodes push while something subscribes to them and otherwise refresh
   on read. User callbacks run only after the current wave of derived values
   has settled. Scope disposal is iterative so deep trees do not use the
   native or JavaScript call stack. *)

type stabilization_diagnostics = {
  stabilization_generation : int;
  stabilization_rounds : int;
  stabilization_effects : int;
  stabilization_dirty_tasks : int;
}

module Rank_map = Map.Make (Int)

type subscription = {
  disposed : bool ref;
  mutable cancel : unit -> unit;
}

type 'a node = {
  mutable callback : 'a -> unit;
  mutable alive : bool;
  disposed : bool ref;
  mutable prev : 'a node option;
  mutable next : 'a node option;
}

type 'a dll = {
  mutable first : 'a node option;
  mutable last : 'a node option;
  mutable count : int;
}

let dll () = { first = None; last = None; count = 0 }

type scheduler = {
  effects : (unit -> unit) Queue.t;
  dirty : (unit -> unit) Queue.t;
  mutable computations : (unit -> unit) Queue.t Rank_map.t;
  observers : (unit -> unit) Queue.t;
  mutable generation : int;
  mutable last : stabilization_diagnostics;
  mutable reconciling : Obj.t list;
}

type 'a signal = {
  owner : scheduler;
  rank : int;
  mutable current : 'a;
  mutable version : int;
  mutable dep1 : int;
  mutable dep2 : int;
  internal : 'a dll;
  users : 'a dll;
  mutable upstream : subscription list;
  mutable disposed : bool;
  mutable watched : bool;
  mutable in_pull : bool;
  tracks_inputs : bool;
  deps : packed list;
  mutable watch : unit -> unit;
  mutable unwatch : unit -> unit;
  mutable apply : unit -> unit;
}

and packed = Pack : 'a signal -> packed

type 'a state = {
  signal : 'a signal;
  mutable pending : 'a option;
  mutable scheduled : bool;
}

type cleanup = {
  mutable run : unit -> unit;
  mutable alive : bool;
  mutable prev : cleanup option;
  mutable next : cleanup option;
}

type owned = {
  sub : subscription;
  mutable alive : bool;
  mutable prev : owned option;
  mutable next : owned option;
}

type scope = {
  id : int;
  name : string;
  mutable clean_first : cleanup option;
  mutable clean_last : cleanup option;
  mutable cleanup_count : int;
  mutable owned_first : owned option;
  mutable owned_last : owned option;
  mutable owned_count : int;
  mutable mounts : (unit -> unit) list;
  mutable unmounts : (unit -> unit) list;
  mutable mounted : bool;
  mutable disposed : bool;
  mutable cleanups_taken : bool;
}

type 'a state_slot = {
  slot_name : string;
  states : (int, 'a state) Hashtbl.t;
}

type 'k switch = {
  switch_subscription : subscription;
  switch_scope : scope ref;
  mutable switch_disposed : bool;
}

type 'k keyed_patch =
  | Insert of 'k * int
  | Remove of 'k * int
  | Move of 'k * int * int

type ('k, 'a) keyed_entry = {
  entry_key : 'k;
  entry_state : 'a state;
  entry_scope : scope;
}

type ('k, 'a) keyed = {
  keyed_subscription : subscription;
  keyed_entries : ('k, 'a) keyed_entry list ref;
  mutable keyed_disposed : bool;
  find_scope : 'k -> scope;
}

type task =
  | Expand of scope
  | Call of (unit -> unit)

let scheduler () =
  {
    effects = Queue.create ();
    dirty = Queue.create ();
    computations = Rank_map.empty;
    observers = Queue.create ();
    generation = 0;
    last =
      {
        stabilization_generation = 0;
        stabilization_rounds = 0;
        stabilization_effects = 0;
        stabilization_dirty_tasks = 0;
      };
    reconciling = [];
  }

let generation owner = owner.generation
let last_stabilization owner = owner.last
let enqueue_effect owner f = Queue.add f owner.effects
let enqueue_dirty owner task = Queue.add task owner.dirty

let schedule_once owner scheduled task =
  if not !scheduled then begin
    scheduled := true;
    enqueue_dirty owner (fun () ->
        scheduled := false;
        task ())
  end

let dispose_subscription (sub : subscription) = sub.cancel ()
let subscription_disposed (sub : subscription) = !(sub.disposed)

let make_subscription cancel =
  let disposed = ref false in
  let sub = { disposed; cancel = (fun () -> ()) } in
  sub.cancel <-
    (fun () ->
      if not !(sub.disposed) then begin
        sub.disposed := true;
        cancel ()
      end);
  sub

let raise_with bt exn =
  (* Melange compiles [raise_with_backtrace] to [caml_restore_raw_backtrace],
     which throws because the primitive is not polyfilled. Native and bytecode
     keep the original backtrace. *)
  match Sys.backend_type with
  | Other "Melange" -> raise exn
  | _ -> Printexc.raise_with_backtrace exn bt

let capture_exn exn = (exn, Printexc.get_raw_backtrace ())

let raise_captured (exn, bt) : 'a = raise_with bt exn

let raise_if = function
  | None -> ()
  | Some captured -> raise_captured captured

let note_exn cell exn =
  if !cell = None then cell := Some (capture_exn exn)

let with_finally finally f =
  match f () with
  | value ->
      finally ();
      value
  | exception exn ->
      let captured = capture_exn exn in
      (try finally () with _ -> ());
      raise_captured captured

let run_tasks queue count run =
  (* A nested stabilize may already have drained this snapshot. Stop on an
     empty queue instead of raising [Queue.Empty], and leave tasks queued
     after [count] for the next round. *)
  let remaining = ref count in
  while !remaining > 0 && not (Queue.is_empty queue) do
    decr remaining;
    run (Queue.take queue)
  done

let prune_computation_queue owner rank queue =
  if Queue.is_empty queue then
    match Rank_map.find_opt rank owner.computations with
    | Some current when current == queue ->
        owner.computations <- Rank_map.remove rank owner.computations
    | _ -> ()

let max_stabilization_rounds = 10000

exception Stabilization_limit_exceeded of int * int * int

let rec schedule_computation (signal : 'a signal) scheduled task =
  if (not !scheduled) && signal.watched && not signal.disposed then begin
    scheduled := true;
    let queue =
      match Rank_map.find_opt signal.rank signal.owner.computations with
      | Some queue -> queue
      | None ->
          let queue = Queue.create () in
          signal.owner.computations <-
            Rank_map.add signal.rank queue signal.owner.computations;
          queue
    in
    Queue.add
      (fun () ->
        scheduled := false;
        if signal.watched && not signal.disposed then
          try task ()
          with exn ->
            let captured = capture_exn exn in
            (* The taken task is otherwise lost; retry on the next stabilize
               without requiring another upstream write. *)
            schedule_computation signal scheduled task;
            raise_captured captured)
      queue
  end

let unlink_sub (dll : 'a dll) (node : 'a node) =
  if node.alive then begin
    node.alive <- false;
    (match node.prev with
    | None -> dll.first <- node.next
    | Some prev -> prev.next <- node.next);
    (match node.next with
    | None -> dll.last <- node.prev
    | Some next -> next.prev <- node.prev);
    (* Keep [next] so an iteration that has already saved this node can reach
       the successor. Clearing it dropped every subscriber after one that was
       cancelled mid-notification. *)
    node.callback <- (fun _ -> ());
    dll.count <- dll.count - 1
  end

let listeners (signal : 'a signal) =
  signal.users.count + signal.internal.count

let disconnect (signal : 'a signal) =
  if signal.watched then begin
    signal.watched <- false;
    let release = signal.unwatch in
    signal.unwatch <- (fun () -> ());
    release ()
  end

(* [connect] and [refresh] walk input edges on a heap stack. A derived chain
   deeper than the stabilization cap must not overflow the native or
   JavaScript call stack, and must not consume that cap. *)
let connect_ref = ref (fun (Pack _signal) -> ())
let refresh_ref = ref (fun (Pack _signal) -> ())

let connect (signal : 'a signal) = !connect_ref (Pack signal)
let refresh (signal : 'a signal) = !refresh_ref (Pack signal)

let consider_listeners (signal : 'a signal) =
  if signal.tracks_inputs && not signal.disposed then
    if listeners signal = 0 then disconnect signal else connect signal

let link ?(connect_now = true) (signal : 'a signal) (dll : 'a dll)
    (callback : 'a -> unit) (disposed : bool ref) : 'a node =
  let node =
    ({ callback; alive = true; disposed; prev = dll.last; next = None }
      : 'a node)
  in
  (match dll.last with
  | None -> dll.first <- Some node
  | Some prev -> prev.next <- Some node);
  dll.last <- Some node;
  dll.count <- dll.count + 1;
  if connect_now && listeners signal = 1 then connect signal;
  node

let cancel_node (signal : 'a signal) (dll : 'a dll) (node : 'a node) =
  if node.alive then begin
    unlink_sub dll node;
    if not signal.disposed then consider_listeners signal
  end

let dispose_signal (signal : 'a signal) =
  if not signal.disposed then begin
    signal.disposed <- true;
    disconnect signal;
    let clear (dll : 'a dll) =
      let node = ref dll.first in
      while
        match !node with
        | None -> false
        | Some (current : 'a node) ->
            node := current.next;
            current.alive <- false;
            current.disposed := true;
            current.callback <- (fun _ -> ());
            current.prev <- None;
            current.next <- None;
            true
      do
        ()
      done;
      dll.first <- None;
      dll.last <- None;
      dll.count <- 0
    in
    clear signal.internal;
    clear signal.users;
    signal.upstream <- []
  end

let notify_internal (signal : 'a signal) value =
  let node = ref signal.internal.first in
  while
    match !node with
    | None -> false
    | Some current ->
        let next = current.next in
        if current.alive && not signal.disposed then current.callback value;
        node := next;
        not signal.disposed
  do
    ()
  done

let notify_users (signal : 'a signal) value stop =
  let error = ref None in
  let node = ref signal.users.first in
  let continue = ref true in
  while !continue do
    match !node with
    | None -> continue := false
    | Some current ->
        let next = current.next in
        let at_stop =
          match stop with None -> false | Some stop -> current == stop
        in
        if current.alive && not signal.disposed then
          (try current.callback value
           with exn -> note_exn error exn);
        if at_stop || signal.disposed then continue := false else node := next
  done;
  raise_if !error

let publish (signal : 'a signal) value =
  if not signal.disposed then begin
    signal.current <- value;
    signal.version <- signal.version + 1;
    notify_internal signal value;
    match signal.users.last with
    | None -> ()
    | Some _ as stop ->
        let delivered = signal.current in
        Queue.add
          (fun () -> notify_users signal delivered stop)
          signal.owner.observers
  end

let run_observers owner =
  let pending = Queue.length owner.observers in
  let seen = ref 0 in
  let error = ref None in
  while !seen < pending && not (Queue.is_empty owner.observers) do
    let task = Queue.take owner.observers in
    incr seen;
    try task () with exn -> note_exn error exn
  done;
  raise_if !error

let stabilize owner =
  let worked = ref false in
  let rounds = ref 0 in
  let effect_count = ref 0 in
  let dirty_count = ref 0 in
  let limit_rounds = ref 0 in
  let continue = ref true in
  (try
     while !continue do
       let effects_n = Queue.length owner.effects in
       let dirty_n = Queue.length owner.dirty in
       let has_comp = not (Rank_map.is_empty owner.computations) in
       if effects_n = 0 && dirty_n = 0 && not has_comp then
         if Queue.is_empty owner.observers then continue := false
         else begin
           (* Observers are not a ranked round: derived values are already
              settled, and the round cap counts effect/dirty fixpoint loops. *)
           worked := true;
           run_observers owner
         end
       else begin
         worked := true;
         incr rounds;
         if effects_n > 0 || dirty_n > 0 then begin
           incr limit_rounds;
           if !limit_rounds > max_stabilization_rounds then
             raise
               (Stabilization_limit_exceeded
                  (max_stabilization_rounds, !effect_count, !dirty_count))
         end;
         run_tasks owner.effects effects_n (fun f ->
             incr effect_count;
             f ());
         let pending_dirty = Queue.length owner.dirty in
         run_tasks owner.dirty pending_dirty (fun task ->
             incr dirty_count;
             task ());
         if
           effects_n = 0 && pending_dirty = 0
           && not (Rank_map.is_empty owner.computations)
         then begin
           let rank, queue = Rank_map.min_binding owner.computations in
           (try
              run_tasks queue (Queue.length queue) (fun task ->
                  incr dirty_count;
                  task ())
            with exn ->
              let captured = capture_exn exn in
              prune_computation_queue owner rank queue;
              raise_captured captured);
           prune_computation_queue owner rank queue
         end
       end
     done
   with exn ->
     let captured = capture_exn exn in
     raise_captured captured);
  if !worked then owner.generation <- owner.generation + 1;
  owner.last <-
    {
      stabilization_generation = owner.generation;
      stabilization_rounds = !rounds;
      stabilization_effects = !effect_count;
      stabilization_dirty_tasks = !dirty_count;
    }

let make_signal owner rank current =
  {
    owner;
    rank;
    current;
    version = 0;
    dep1 = 0;
    dep2 = 0;
    internal = dll ();
    users = dll ();
    upstream = [];
    disposed = false;
    watched = false;
    in_pull = false;
    tracks_inputs = false;
    deps = [];
    watch = (fun () -> ());
    unwatch = (fun () -> ());
    apply = (fun () -> ());
  }

let constant owner initial = make_signal owner 0 initial

let sample (signal : 'a signal) =
  if signal.tracks_inputs && (not signal.watched) && not signal.disposed
     && not signal.in_pull
  then refresh signal;
  signal.current

let get = sample

let subscribe ?(emit_initial = true) (signal : 'a signal) callback =
  if signal.disposed then invalid_arg "cannot observe a disposed signal";
  let disposed = ref false in
  let node = link signal signal.users callback disposed in
  let sub = { disposed; cancel = (fun () -> ()) } in
  sub.cancel <-
    (fun () ->
      if not !(sub.disposed) then begin
        sub.disposed := true;
        cancel_node signal signal.users node
      end);
  if emit_initial then
    (try callback (sample signal)
     with exn ->
       let captured = capture_exn exn in
       dispose_subscription sub;
       raise_captured captured);
  sub

let subscribe_internal (signal : 'a signal) callback =
  if not signal.disposed then begin
    let disposed = ref false in
    let node = link ~connect_now:false signal signal.internal callback disposed in
    let sub = { disposed; cancel = (fun () -> ()) } in
    sub.cancel <-
      (fun () ->
        if not !(sub.disposed) then begin
          sub.disposed := true;
          cancel_node signal signal.internal node
        end);
    sub
  end
  else { disposed = ref true; cancel = (fun () -> ()) }

let observe signal callback = subscribe ~emit_initial:true signal callback

let state owner initial =
  { signal = constant owner initial; pending = None; scheduled = false }

let value state_value = state_value.signal
let get_state state_value = sample (value state_value)

let set (state_value : 'a state) next_value =
  let signal = value state_value in
  if signal.disposed then state_value.pending <- None
  else begin
    state_value.pending <- Some next_value;
    if not state_value.scheduled then begin
      state_value.scheduled <- true;
      enqueue_dirty signal.owner (fun () ->
          state_value.scheduled <- false;
          match state_value.pending with
          | Some pending_value when not signal.disposed ->
              state_value.pending <- None;
              publish signal pending_value
          | _ -> state_value.pending <- None)
    end
  end

let update state_value update_fn =
  let current =
    match state_value.pending with
    | Some pending_value -> pending_value
    | None -> get_state state_value
  in
  set state_value (update_fn current)

let remember (signal : 'a signal) subs =
  signal.upstream <- subs;
  signal.unwatch <-
    (fun () ->
      List.iter dispose_subscription signal.upstream;
      signal.upstream <- [])

let rollback_visit visiting =
  List.iter
    (fun (Pack node) ->
      node.in_pull <- false;
      if node.watched then begin
        node.watched <- false;
        let release = node.unwatch in
        node.unwatch <- (fun () -> ());
        try release () with _ -> ()
      end)
    visiting

let should_enter node force =
  node.tracks_inputs && (not node.disposed) && (not node.in_pull)
  && (force || not node.watched)

let refresh_from (Pack root) =
  let visiting = ref [] in
  let stack = ref [ `Enter (Pack root, true) ] in
  try
    while !stack <> [] do
      match List.hd !stack with
      | `Enter ((Pack node as packed), force) ->
          stack := List.tl !stack;
          if should_enter node force then begin
            node.in_pull <- true;
            visiting := packed :: !visiting;
            let children = List.map (fun dep -> `Enter (dep, false)) node.deps in
            stack := children @ (`Leave packed :: !stack)
          end
      | `Leave (Pack node) ->
          stack := List.tl !stack;
          node.apply ();
          node.in_pull <- false
    done
  with exn ->
    let captured = capture_exn exn in
    List.iter (fun (Pack node) -> node.in_pull <- false) !visiting;
    raise_captured captured

let connect_from (Pack root) =
  if root.tracks_inputs && (not root.watched) && not root.disposed then
    let visiting = ref [] in
    let stack = ref [ `Enter (Pack root, true) ] in
    try
      while !stack <> [] do
        match List.hd !stack with
        | `Enter ((Pack node as packed), force) ->
            stack := List.tl !stack;
            if should_enter node force then begin
              node.in_pull <- true;
              node.watched <- true;
              visiting := packed :: !visiting;
              (try node.watch ()
               with exn ->
                 node.watched <- false;
                 raise exn);
              let children =
                List.map (fun dep -> `Enter (dep, false)) node.deps
              in
              stack := children @ (`Leave packed :: !stack)
            end
        | `Leave (Pack node) ->
            stack := List.tl !stack;
            node.apply ();
            node.in_pull <- false
      done
    with exn ->
      let captured = capture_exn exn in
      rollback_visit !visiting;
      raise_captured captured

let () =
  refresh_ref := refresh_from;
  connect_ref := connect_from

let changed_current (derived : 'a signal) next =
  if next != derived.current then begin
    derived.current <- next;
    derived.version <- derived.version + 1
  end

let map transform (source : 'a signal) =
  let derived =
    make_signal source.owner (source.rank + 1) (transform (sample source))
  in
  derived.dep1 <- source.version;
  let derived = { derived with tracks_inputs = true; deps = [ Pack source ] } in
  let scheduled = ref false in
  let recompute () = publish derived (transform (sample source)) in
  derived.watch <-
    (fun () ->
      let sub =
        subscribe_internal source (fun _ ->
            schedule_computation derived scheduled recompute)
      in
      remember derived [ sub ]);
  derived.apply <-
    (fun () ->
      if source.version <> derived.dep1 then begin
        let next = transform source.current in
        derived.dep1 <- source.version;
        changed_current derived next
      end);
  derived

let map2 transform (left : 'a signal) (right : 'b signal) =
  if left.owner != right.owner then
    invalid_arg "map inputs must share one scheduler";
  if left.disposed || right.disposed then
    invalid_arg "cannot derive from a disposed signal";
  let initial = transform (sample left) (sample right) in
  if left.disposed || right.disposed then
    invalid_arg "cannot derive from a disposed signal";
  let derived =
    make_signal left.owner (max left.rank right.rank + 1) initial
  in
  derived.dep1 <- left.version;
  derived.dep2 <- right.version;
  let derived =
    { derived with tracks_inputs = true; deps = [ Pack left; Pack right ] }
  in
  let scheduled = ref false in
  let recompute () = publish derived (transform (sample left) (sample right)) in
  derived.watch <-
    (fun () ->
      let subs = ref [] in
      let connect_one input =
        let sub =
          subscribe_internal input (fun _ ->
              schedule_computation derived scheduled recompute)
        in
        subs := sub :: !subs
      in
      (try
         connect_one left;
         connect_one right
       with exn ->
         let captured = capture_exn exn in
         List.iter dispose_subscription !subs;
         raise_captured captured);
      remember derived !subs);
  derived.apply <-
    (fun () ->
      if left.version <> derived.dep1 || right.version <> derived.dep2 then begin
        let next = transform left.current right.current in
        derived.dep1 <- left.version;
        derived.dep2 <- right.version;
        changed_current derived next
      end);
  derived

let cutoff equal (source : 'a signal) =
  let derived = make_signal source.owner (source.rank + 1) (sample source) in
  derived.dep1 <- source.version;
  let derived = { derived with tracks_inputs = true; deps = [ Pack source ] } in
  let scheduled = ref false in
  let publish_if_changed next =
    derived.dep1 <- source.version;
    if not (equal derived.current next) then publish derived next
  in
  derived.watch <-
    (fun () ->
      let sub =
        subscribe_internal source (fun _ ->
            schedule_computation derived scheduled (fun () ->
                publish_if_changed (sample source)))
      in
      remember derived [ sub ]);
  derived.apply <-
    (fun () ->
      if source.version <> derived.dep1 then begin
        let next = source.current in
        derived.dep1 <- source.version;
        if not (equal derived.current next) then changed_current derived next
      end);
  derived

let phys_equal a b = a == b

let next_scope_id = Atomic.make 0

let fresh_scope_id () = Atomic.fetch_and_add next_scope_id 1 + 1

let make_scope name =
  {
    id = fresh_scope_id ();
    name;
    clean_first = None;
    clean_last = None;
    cleanup_count = 0;
    owned_first = None;
    owned_last = None;
    owned_count = 0;
    mounts = [];
    unmounts = [];
    mounted = false;
    disposed = false;
    cleanups_taken = false;
  }

let scope name = make_scope name
let scope_name scope_value = scope_value.name
let scope_id scope_value = scope_value.id
let scope_disposed (scope_value : scope) = scope_value.disposed
let scope_mounted scope_value = scope_value.mounted
let scope_cleanup_count scope_value = scope_value.cleanup_count
let scope_owned_count scope_value = scope_value.owned_count

let unlink_cleanup (scope_value : scope) (node : cleanup) =
  if node.alive then begin
    node.alive <- false;
    node.run <- (fun () -> ());
    if not scope_value.cleanups_taken then begin
      (match node.prev with
      | None -> scope_value.clean_first <- node.next
      | Some prev -> prev.next <- node.next);
      (match node.next with
      | None -> scope_value.clean_last <- node.prev
      | Some next -> next.prev <- node.prev);
      node.prev <- None;
      node.next <- None;
      scope_value.cleanup_count <- scope_value.cleanup_count - 1
    end
  end

let register_cleanup (scope_value : scope) callback =
  if scope_value.disposed then begin
    callback ();
    { disposed = ref true; cancel = (fun () -> ()) }
  end
  else begin
    let node =
      {
        run = callback;
        alive = true;
        prev = scope_value.clean_last;
        next = None;
      }
    in
    (match scope_value.clean_last with
    | None -> scope_value.clean_first <- Some node
    | Some prev -> prev.next <- Some node);
    scope_value.clean_last <- Some node;
    scope_value.cleanup_count <- scope_value.cleanup_count + 1;
    let sub = { disposed = ref false; cancel = (fun () -> ()) } in
    sub.cancel <-
      (fun () ->
        if not !(sub.disposed) then begin
          sub.disposed := true;
          unlink_cleanup scope_value node
        end);
    sub
  end

(* Iterative disposal. [disposing] is only set while the outermost call is
   draining the heap stack; it does not retain scopes afterwards. The runtime
   graph is single-domain aside from {!fresh_scope_id}. *)
let dispose_stack = ref []
let disposing = ref false
let dispose_error = ref None

let push_fifo calls =
  let pending = ref (List.rev calls) in
  while !pending <> [] do
    dispose_stack := Call (List.hd !pending) :: !dispose_stack;
    pending := List.tl !pending
  done

let take_cleanups (scope_value : scope) =
  let calls = ref [] in
  let node = ref scope_value.clean_first in
  while
    match !node with
    | None -> false
    | Some (current : cleanup) ->
        node := current.next;
        current.prev <- None;
        current.next <- None;
        calls :=
          (fun () ->
            (* Read [alive] at call time so an earlier cleanup can cancel
               this one during disposal. *)
            if current.alive then begin
              current.alive <- false;
              let run = current.run in
              current.run <- (fun () -> ());
              run ()
            end)
          :: !calls;
        true
  do
    ()
  done;
  scope_value.clean_first <- None;
  scope_value.clean_last <- None;
  scope_value.cleanup_count <- 0;
  scope_value.cleanups_taken <- true;
  List.rev !calls

let unlink_owned (scope_value : scope) (node : owned) =
  if node.alive then begin
    node.alive <- false;
    (match node.prev with
    | None -> scope_value.owned_first <- node.next
    | Some prev -> prev.next <- node.next);
    (match node.next with
    | None -> scope_value.owned_last <- node.prev
    | Some next -> next.prev <- node.prev);
    node.prev <- None;
    node.next <- None;
    scope_value.owned_count <- scope_value.owned_count - 1
  end

let take_owned (scope_value : scope) =
  let subs = ref [] in
  let node = ref scope_value.owned_first in
  while
    match !node with
    | None -> false
    | Some (current : owned) ->
        node := current.next;
        current.alive <- false;
        current.prev <- None;
        current.next <- None;
        subs := current.sub :: !subs;
        true
  do
    ()
  done;
  scope_value.owned_first <- None;
  scope_value.owned_last <- None;
  scope_value.owned_count <- 0;
  List.rev !subs

let expand_scope (scope_value : scope) =
  let cleanups = take_cleanups scope_value in
  let owned = take_owned scope_value in
  let unmounts = List.rev scope_value.unmounts in
  scope_value.mounts <- [];
  scope_value.unmounts <- [];
  push_fifo unmounts;
  push_fifo (List.map (fun sub () -> dispose_subscription sub) owned);
  push_fifo cleanups

let dispose_scope (scope_value : scope) =
  if not scope_value.disposed then begin
    scope_value.disposed <- true;
    scope_value.mounted <- false;
    dispose_stack := Expand scope_value :: !dispose_stack;
    if not !disposing then begin
      disposing := true;
      dispose_error := None;
      (try
         while !dispose_stack <> [] do
           let task = List.hd !dispose_stack in
           dispose_stack := List.tl !dispose_stack;
           match task with
           | Expand scope_value -> expand_scope scope_value
           | Call f -> (
               try f ()
               with exn -> note_exn dispose_error exn)
         done
       with exn -> note_exn dispose_error exn);
      disposing := false;
      dispose_stack := [];
      let error = !dispose_error in
      dispose_error := None;
      raise_if error
    end
  end

let child_scope name (parent : scope) =
  if parent.disposed then invalid_arg "cannot create a child of a disposed scope";
  let child = make_scope name in
  let _owned =
    let cleanup = register_cleanup parent (fun () -> dispose_scope child) in
    (* Inline [own] would recurse; attach the handle directly. *)
    let node =
      { sub = cleanup; alive = true; prev = child.owned_last; next = None }
    in
    (match child.owned_last with
    | None -> child.owned_first <- Some node
    | Some prev -> prev.next <- Some node);
    child.owned_last <- Some node;
    child.owned_count <- child.owned_count + 1;
    let original = cleanup.cancel in
    cleanup.cancel <-
      (fun () ->
        unlink_owned child node;
        original ());
    cleanup
  in
  child

let own (scope_value : scope) (sub : subscription) =
  if scope_value.disposed then begin
    dispose_subscription sub;
    sub
  end
  else begin
    let node =
      { sub; alive = true; prev = scope_value.owned_last; next = None }
    in
    (match scope_value.owned_last with
    | None -> scope_value.owned_first <- Some node
    | Some prev -> prev.next <- Some node);
    scope_value.owned_last <- Some node;
    scope_value.owned_count <- scope_value.owned_count + 1;
    let original = sub.cancel in
    sub.cancel <-
      (fun () ->
        unlink_owned scope_value node;
        original ());
    sub
  end

let on_mount (scope_value : scope) callback =
  if scope_value.disposed then ()
  else if scope_value.mounted then callback ()
  else scope_value.mounts <- callback :: scope_value.mounts

let on_unmount (scope_value : scope) callback =
  if not scope_value.disposed then
    scope_value.unmounts <- callback :: scope_value.unmounts

let on_dispose scope_value callback =
  ignore (register_cleanup scope_value callback)

let own_signal scope_value signal =
  on_dispose scope_value (fun () -> dispose_signal signal);
  signal

let mount (scope_value : scope) =
  if (not scope_value.disposed) && not scope_value.mounted then begin
    scope_value.mounted <- true;
    let callbacks = List.rev scope_value.mounts in
    scope_value.mounts <- [];
    let error = ref None in
    List.iter
      (fun callback ->
        if not scope_value.disposed then
          try callback () with exn -> note_exn error exn)
      callbacks;
    raise_if !error
  end

let active (scope_value : scope) =
  scope_value.mounted && not scope_value.disposed

let state_slot name = { slot_name = name; states = Hashtbl.create 8 }

let state_slot_count slot = Hashtbl.length slot.states

let state_at scheduler (scope_value : scope) slot initial =
  if scope_value.disposed then
    invalid_arg "cannot create state in a disposed scope";
  match Hashtbl.find_opt slot.states scope_value.id with
  | Some existing when existing.signal.owner != scheduler ->
      invalid_arg
        ("state_at: slot " ^ slot.slot_name
       ^ " is owned by a different scheduler")
  | Some existing -> existing
  | None ->
      let created = state scheduler initial in
      Hashtbl.replace slot.states scope_value.id created;
      on_dispose scope_value (fun () ->
          dispose_signal (value created);
          Hashtbl.remove slot.states scope_value.id);
      created

let run_calls calls =
  let error = ref None in
  List.iter
    (fun call -> try call () with exn -> note_exn error exn)
    calls;
  raise_if !error

let switch_scope switch_value = !(switch_value.switch_scope)
let switch_disposed switch_value = switch_value.switch_disposed
let switch_subscription switch_value = switch_value.switch_subscription

let dispose_switch switch_value =
  if not switch_value.switch_disposed then begin
    switch_value.switch_disposed <- true;
    run_calls
      [
        (fun () -> dispose_subscription switch_value.switch_subscription);
        (fun () -> dispose_scope !(switch_value.switch_scope));
      ]
  end

let own_lifetime (parent : scope) cleanup =
  let sub = { disposed = ref false; cancel = (fun () -> ()) } in
  sub.cancel <-
    (fun () ->
      if not !(sub.disposed) then begin
        sub.disposed := true;
        cleanup ()
      end);
  ignore (own parent sub);
  fun () -> dispose_subscription sub

let switch (parent : scope) (source : 'key signal) equal mount_scope =
  if parent.disposed then invalid_arg "cannot switch in a disposed scope";
  if source.disposed then invalid_arg "cannot switch from a disposed signal";
  let initial_key = sample source in
  let current_key = ref initial_key in
  let current_scope = ref (scope "switch:initializing") in
  let disposed = ref false in
  let cancelled = ref (fun () -> ()) in
  let pending = ref None in
  let busy = ref true in
  let replace next_key =
    if
      !disposed
      || ((not !current_scope.disposed) && equal !current_key next_key)
    then ()
    else begin
      dispose_scope !current_scope;
      if not !disposed then begin
        let next_scope = mount_scope next_key in
        if !disposed then dispose_scope next_scope
        else begin
          current_key := next_key;
          current_scope := next_scope;
          try mount next_scope
          with exn ->
            let captured = capture_exn exn in
            (try dispose_scope next_scope with _ -> ());
            raise_captured captured
        end
      end
    end
  in
  let drain () =
    busy := true;
    with_finally
      (fun () -> busy := false)
      (fun () ->
        while (not (!pending = None)) && not !disposed do
          match !pending with
          | None -> ()
          | Some next_key ->
              pending := None;
              replace next_key
        done)
  in
  let observer =
    subscribe ~emit_initial:false source (fun next_key ->
        if not !disposed then begin
          pending := Some next_key;
          if not !busy then drain ()
        end)
  in
  let subscription =
    {
      observer with
      cancel =
        (fun () ->
          pending := None;
          run_calls [ observer.cancel; (fun () -> !cancelled ()) ]);
    }
  in
  let switch_value =
    {
      switch_subscription = subscription;
      switch_scope = current_scope;
      switch_disposed = false;
    }
  in
  try
    cancelled :=
      own_lifetime parent (fun () ->
          disposed := true;
          dispose_switch switch_value);
    let initial_scope = mount_scope initial_key in
    dispose_scope !current_scope;
    current_scope := initial_scope;
    if !disposed then dispose_scope initial_scope else mount initial_scope;
    drain ();
    switch_value
  with exn ->
    let captured = capture_exn exn in
    disposed := true;
    (try !cancelled () with _ -> ());
    (try dispose_scope !current_scope with _ -> ());
    raise_captured captured

let entry_key entry = entry.entry_key
let entry_scope entry = entry.entry_scope
let entry_state entry = entry.entry_state

let key_index (type key) (items : 'item list) (key_fn : 'item -> key)
    (compare : key -> key -> int) : key -> int option =
  let module Key_map = Map.Make (struct
    type t = key

    let compare = compare
  end) in
  let items = Array.of_list items in
  let rec loop index indexes =
    if index = Array.length items then indexes
    else
      let key = key_fn items.(index) in
      if Key_map.mem key indexes then
        invalid_arg "keyed collection contains a duplicate key"
      else loop (index + 1) (Key_map.add key index indexes)
  in
  let indexes = loop 0 Key_map.empty in
  fun key -> Key_map.find_opt key indexes

let set_keyed_item item_state current =
  let staged =
    match item_state.pending with
    | Some pending -> pending
    | None -> get_state item_state
  in
  (* Physical inequality. On JavaScript, [!=] compiles to [!==], which compares
     strings and numbers by value; native OCaml compares them by identity.
     Payloads that contain functions cannot use structural equality. *)
  if staged != current then set item_state current

let remaining_counts size = Array.init (size + 1) (fun index -> index land -index)

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
    (key_fn : 'item -> key) (compare : key -> key -> int)
    (remember : key -> scope -> unit) (forget : key -> unit) mount_scope on_patch
    =
  let module Key_map = Map.Make (struct
    type t = key

    let compare = compare
  end) in
  let new_index = key_index items key_fn compare in
  let first_error = ref None in
  let attempt f =
    try Some (f ())
    with exn ->
      note_exn first_error exn;
      None
  in
  let release entry =
    forget entry.entry_key;
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
      (0, [], [])
      !entries_ref
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
          | None -> (
              let item_state = state scheduler current in
              match attempt (fun () -> mount_scope (value item_state)) with
              | None ->
                  dispose_signal (value item_state);
                  None
              | Some child -> (
                  let entry =
                    {
                      entry_key = key;
                      entry_state = item_state;
                      entry_scope = child;
                    }
                  in
                  created := entry :: !created;
                  ignore (own_signal child (value item_state));
                  if stopped () then (
                    release entry;
                    None)
                  else
                    match attempt (fun () -> mount child) with
                    | None ->
                        release entry;
                        None
                    | Some () ->
                        if stopped () then (
                          release entry;
                          None)
                        else (
                          remember key child;
                          patch (Insert (key, index));
                          Some entry)))
        in
        match entry with
        | None ->
            (* A failed mount is not dropped on the floor: the caller retries
               this target. Stop the walk so later rows are retried with it. *)
            if !first_error <> None then List.rev result
            else loop index result rest
        | Some entry -> loop (index + 1) (entry :: result) rest
  in
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
    end
    else entries_ref := result
  in
  (try finish ()
   with exn ->
     (* A later comparator or accounting failure must not replace the first
        user-callback exception. Roll the in-flight rows back either way. *)
     note_exn first_error exn;
     List.iter release !created;
     entries_ref :=
       List.filter (fun entry -> not entry.entry_scope.disposed) retained);
  raise_if !first_error

let reconcile_keyed_impl stopped scheduler entries_ref items key_fn compare
    remember forget mount_scope on_patch =
  let identity = Obj.repr entries_ref in
  if List.exists (fun running -> running == identity) scheduler.reconciling then
    invalid_arg "cannot recursively reconcile the same entries";
  scheduler.reconciling <- identity :: scheduler.reconciling;
  with_finally
    (fun () ->
      scheduler.reconciling <-
        List.filter (fun running -> running != identity) scheduler.reconciling)
    (fun () ->
      reconcile_keyed_unguarded stopped scheduler entries_ref items key_fn
        compare remember forget mount_scope on_patch)

let reconcile_keyed scheduler entries_ref items key_fn compare mount_scope
    on_patch =
  reconcile_keyed_impl
    (fun () -> false)
    scheduler entries_ref items key_fn compare
    (fun _ _ -> ())
    (fun _ -> ())
    mount_scope on_patch

let keyed_entries keyed_value = !(keyed_value.keyed_entries)

let dispose_keyed keyed_value =
  if not keyed_value.keyed_disposed then begin
    keyed_value.keyed_disposed <- true;
    let entries = !(keyed_value.keyed_entries) in
    keyed_value.keyed_entries := [];
    run_calls
      ((fun () -> dispose_subscription keyed_value.keyed_subscription)
      :: List.map
           (fun entry () -> dispose_scope entry.entry_scope)
           entries)
  end

let keyed (type key) (parent : scope) (source : 'item signal) key_fn compare mount_scope
    on_patch =
  if parent.disposed then
    invalid_arg "cannot create a keyed collection in a disposed scope";
  if source.disposed then invalid_arg "cannot reconcile a disposed signal";
  (* Lookup is ordered by the caller's comparator. A hash table cannot do
     that: comparators such as "equal when the strings have the same length"
     do not agree with [Hashtbl.hash]. A balanced tree is O(log n). *)
  let module Table = Map.Make (struct
    type t = key

    let compare = compare
  end) in
  let scopes = ref Table.empty in
  let remember key scope_value = scopes := Table.add key scope_value !scopes in
  let forget key = scopes := Table.remove key !scopes in
  let scheduler = source.owner in
  let entries_ref = ref [] in
  let disposed = ref false in
  let reconciling = ref false in
  let pending = ref None in
  let retry_scheduled = ref false in
  let rec reconcile items =
    pending := Some items;
    if not !reconciling then begin
      reconciling := true;
      with_finally
        (fun () -> reconciling := false)
        (fun () ->
          let first_error = ref None in
          while (not (!pending = None)) && not !disposed do
            let items = Option.get !pending in
            pending := None;
            try
              reconcile_keyed_impl
                (fun () -> !disposed)
                scheduler entries_ref items key_fn compare remember forget
                mount_scope on_patch
            with exn ->
              note_exn first_error exn;
              (* Retry a failed mount on a later stabilize. Running it in
                 this turn would loop when the mount keeps failing.
                 Duplicate-key rejection is permanent for that target, so
                 retrying it would hide the next edit. *)
              let retry =
                match !first_error with
                | Some (Invalid_argument _, _) -> false
                | Some _ -> true
                | None -> false
              in
              if (not !disposed) && retry then
                schedule_once scheduler retry_scheduled (fun () ->
                    if not !disposed then reconcile (sample source))
          done;
          pending := None;
          raise_if !first_error)
    end
  in
  let subscription = subscribe ~emit_initial:false source reconcile in
  let cancelled = ref (fun () -> ()) in
  let wrapped =
    {
      subscription with
      cancel =
        (fun () ->
          pending := None;
          run_calls [ subscription.cancel; (fun () -> !cancelled ()) ]);
    }
  in
  let keyed_value =
    {
      keyed_subscription = wrapped;
      keyed_entries = entries_ref;
      keyed_disposed = false;
      find_scope =
        (fun key ->
          try Table.find key !scopes
          with Not_found -> invalid_arg "keyed scope not found");
    }
  in
  cancelled :=
    own_lifetime parent (fun () ->
        disposed := true;
        dispose_keyed keyed_value);
  try
    if not !disposed then reconcile (sample source);
    keyed_value
  with exn ->
    let captured = capture_exn exn in
    (try dispose_keyed keyed_value with _ -> ());
    raise_captured captured

let keyed_find_scope keyed_value key =
  if keyed_value.keyed_disposed then invalid_arg "keyed scope not found";
  keyed_value.find_scope key
