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
  generation_value : int ref;
  last_stabilization_value : stabilization_diagnostics ref;
}

type subscription = { disposed : bool ref; cancel : unit -> unit }

type 'value subscriber = {
  subscriber_id : int;
  mutable callback : 'value -> unit;
  subscriber_disposed : bool ref;
  mutable previous : 'value subscriber option;
  mutable next : 'value subscriber option;
}

type 'value subscribers = {
  mutable first : 'value subscriber option;
  mutable last : 'value subscriber option;
}

type cleanup_entry = { cleanup_id : int; cleanup_callback : unit -> unit }

type 'value signal = {
  owner : scheduler;
  rank : int;
  current : 'value ref;
  next_subscriber_id : int ref;
  subscribers : 'value subscribers;
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

let scheduler () =
  {
    effects = Queue.create ();
    dirty = Queue.create ();
    computations = ref Rank_map.empty;
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

let schedule_computation reactive scheduled task =
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
        if not !(reactive.disposed_signal) then task ())
      queue
  end

let run_tasks queue count run =
  (* Taking only the snapshot length leaves new work for the next round and
     keeps unexecuted tasks queued if a callback raises. *)
  for _ = 1 to count do
    run (Queue.take queue)
  done

let prune_computation_queue owner rank queue =
  if Queue.is_empty queue then
    owner.computations := Rank_map.remove rank !(owner.computations)

let max_stabilization_rounds = 10000

exception Stabilization_limit_exceeded of int * int * int

let stabilize owner =
  let worked = ref false in
  let rounds = ref 0 in
  let effect_count = ref 0 in
  let dirty_count = ref 0 in
  let continue = ref true in
  while !continue do
    let effects = Queue.length owner.effects in
    let dirty = Queue.length owner.dirty in
    if effects = 0 && dirty = 0 && Rank_map.is_empty !(owner.computations) then
      continue := false
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
      if
        effects = 0 && pending_dirty = 0
        && not (Rank_map.is_empty !(owner.computations))
      then begin
        (* Settle mutations first, then recompute one dependency rank per round. *)
        let rank, queue = Rank_map.min_binding !(owner.computations) in
        (try
          run_tasks queue (Queue.length queue) (fun task ->
              incr dirty_count;
              task ())
        with exn ->
          prune_computation_queue owner rank queue;
          raise exn);
        prune_computation_queue owner rank queue
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

let constant owner initial =
  {
    owner;
    rank = 0;
    current = ref initial;
    next_subscriber_id = ref 0;
    subscribers = { first = None; last = None };
    upstream_subscriptions = ref [];
    disposed_signal = ref false;
  }

let sample reactive = !(reactive.current)

let get = sample

let subscribe ?(emit_initial = true) reactive callback =
  if !(reactive.disposed_signal) then
    invalid_arg "cannot observe a disposed signal";
  incr reactive.next_subscriber_id;
  let subscriber_id = !(reactive.next_subscriber_id) in
  let disposed = ref false in
  let subscriber_value =
    {
      subscriber_id;
      callback;
      subscriber_disposed = disposed;
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
      subscriber_value.callback <- ignore
    end
  in
  (match reactive.subscribers.last with
  | None -> reactive.subscribers.first <- Some subscriber_value
  | Some previous -> previous.next <- Some subscriber_value);
  reactive.subscribers.last <- Some subscriber_value;
  if emit_initial then
    (try callback (sample reactive) with exn -> cancel (); raise exn);
  { disposed; cancel }

let observe reactive callback = subscribe ~emit_initial:true reactive callback

let dispose_subscription subscription = subscription.cancel ()

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
    reactive.subscribers.last <- None
  end

let publish reactive next_value =
  if not !(reactive.disposed_signal) then begin
    reactive.current := next_value;
    let rec snapshot accumulated = function
      | None -> List.rev accumulated
      | Some subscriber -> snapshot (subscriber :: accumulated) subscriber.next
    in
    List.iter
      (fun subscriber_value ->
        if not !(reactive.disposed_signal) then
          subscriber_value.callback next_value)
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

let map transform source =
  let derived =
    {
      (constant source.owner (transform (sample source))) with
      rank = source.rank + 1;
    }
  in
  let scheduled = ref false in
  let subscription =
    subscribe ~emit_initial:false source (fun _current ->
        schedule_computation derived scheduled (fun () ->
            publish derived (transform (sample source))))
  in
  derived.upstream_subscriptions :=
    !(derived.upstream_subscriptions) @ [ subscription ];
  derived

let map2 transform left right =
  if left.owner != right.owner then
    invalid_arg "map inputs must share one scheduler";
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
  derived.upstream_subscriptions :=
    !(derived.upstream_subscriptions)
    @ [
        subscribe ~emit_initial:false left recompute;
        subscribe ~emit_initial:false right recompute;
      ];
  derived

let cutoff equal source =
  let derived =
    { (constant source.owner (sample source)) with rank = source.rank + 1 }
  in
  let scheduled = ref false in
  derived.upstream_subscriptions :=
    !(derived.upstream_subscriptions)
    @ [
        subscribe ~emit_initial:false source (fun _ ->
            schedule_computation derived scheduled (fun () ->
                let next_value = sample source in
                if not (equal (sample derived) next_value) then
                  publish derived next_value));
      ];
  derived

let next_scope_id = ref 0

let make_scope name =
  incr next_scope_id;
  {
    scope_id = !next_scope_id;
    scope_name = name;
    next_cleanup_id = ref 0;
    cleanup_callbacks = ref [];
    mount_callbacks = ref [];
    unmount_callbacks = ref [];
    owned_subscriptions = ref [];
    mounted = ref false;
    disposed_scope = ref false;
  }

let register_cleanup scope_value callback =
  if !(scope_value.disposed_scope) then begin
    callback ();
    { disposed = ref true; cancel = (fun () -> ()) }
  end
  else begin
    incr scope_value.next_cleanup_id;
    let cleanup_id = !(scope_value.next_cleanup_id) in
    let disposed = ref false in
    let entry = {
      cleanup_id;
      cleanup_callback = (fun () ->
        if not !disposed then begin
          disposed := true;
          callback ()
        end);
    } in
    let cancel () =
      if !disposed then ()
      else begin
        disposed := true;
        scope_value.cleanup_callbacks :=
          List.filter
            (fun current -> current.cleanup_id <> cleanup_id)
            !(scope_value.cleanup_callbacks)
      end
    in
    scope_value.cleanup_callbacks := entry :: !(scope_value.cleanup_callbacks);
    { disposed; cancel }
  end

let run_cleanups callbacks =
  let first_error = ref None in
  List.iter
    (fun callback ->
      try callback ()
      with exn -> if !first_error = None then first_error := Some exn)
    callbacks;
  match !first_error with None -> () | Some exn -> raise exn

let rec child_scope name parent =
  if !(parent.disposed_scope)
  then invalid_arg "cannot create a child of a disposed scope";
  let child = make_scope name in
  ignore
    (own child (register_cleanup parent (fun () -> dispose_scope child)));
  child

and own scope_value subscription =
  if !(scope_value.disposed_scope) then dispose_subscription subscription
  else
    scope_value.owned_subscriptions :=
      subscription :: !(scope_value.owned_subscriptions);
  subscription

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
    List.iter (fun callback -> callback ()) callbacks
  end

let active scope_value =
  !(scope_value.mounted) && not !(scope_value.disposed_scope)

let state_slot name = { slot_name = name; slot_states = Hashtbl.create 8 }

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
    dispose_subscription switch_value.switch_subscription;
    dispose_scope !(switch_value.switch_scope)
  end

let switch parent source equal mount_scope =
  if !(parent.disposed_scope) then
    invalid_arg "cannot switch in a disposed scope";
  let initial_key = sample source in
  let current_key = ref initial_key in
  let current_scope = ref (mount_scope initial_key) in
  let disposed = ref false in
  mount !current_scope;
  let subscription =
    subscribe ~emit_initial:false source (fun next_key ->
        if !disposed || equal !current_key next_key then ()
        else begin
          dispose_scope !current_scope;
          let next_scope = mount_scope next_key in
          current_key := next_key;
          current_scope := next_scope;
          mount next_scope
        end)
  in
  let switch_value =
    {
      switch_subscription = subscription;
      switch_scope = current_scope;
      switch_disposed = disposed;
    }
  in
  ignore
    (own parent { disposed; cancel = (fun () -> dispose_switch switch_value) });
  switch_value

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

let reconcile_keyed (type key) scheduler
    (entries_ref : (key, 'item) keyed_entry list ref) (items : 'item list)
    (key_fn : 'item -> key) (compare : key -> key -> int) mount_scope on_patch =
  let module Key_map = Map.Make (struct
    type t = key

    let compare = compare
  end) in
  let new_index = key_index items key_fn compare in
  let _, retained, removed =
    List.fold_left
      (fun (index, retained, removed) entry ->
        match new_index entry.entry_key with
        | Some _ -> (index + 1, entry :: retained, removed)
        | None -> (index + 1, retained, (index, entry) :: removed))
      (0, [], []) !entries_ref
  in
  List.iter
    (fun (index, entry) ->
      on_patch (Remove (entry.entry_key, index));
      dispose_scope entry.entry_scope)
    removed;
  let retained = List.rev retained in
  let size, existing =
    List.fold_left
      (fun (index, entries) entry ->
        (index + 1, Key_map.add entry.entry_key (index, entry) entries))
      (0, Key_map.empty) retained
  in
  (* Unprocessed retained entries stay in their original order. A Fenwick tree
     counts those before each key, giving the current Move index in O(log n)
     without searching or repeatedly rebuilding the visible list. *)
  let counts = remaining_counts size in
  let rec loop index result = function
    | [] -> List.rev result
    | current :: rest ->
        let key = key_fn current in
        let entry =
          match Key_map.find_opt key existing with
          | Some (original_index, entry) ->
              set_keyed_item entry.entry_state current;
              let before = remaining_before counts original_index in
              if before <> 0 then on_patch (Move (key, index + before, index));
              remove_remaining counts original_index;
              entry
          | None ->
              let item_state = state scheduler current in
              let child = mount_scope (value item_state) in
              ignore (own_signal child (value item_state));
              let entry =
                {
                  entry_key = key;
                  entry_state = item_state;
                  entry_scope = child;
                }
              in
              mount child;
              on_patch (Insert (key, index));
              entry
        in
        loop (index + 1) (entry :: result) rest
  in
  entries_ref := loop 0 [] items

let dispose_keyed keyed_value =
  if !(keyed_value.keyed_disposed) then ()
  else begin
    keyed_value.keyed_disposed := true;
    dispose_subscription keyed_value.keyed_subscription;
    run_cleanups
      (List.map
         (fun entry -> fun () -> dispose_scope entry.entry_scope)
         !(keyed_value.keyed_entries))
  end

let keyed parent source key_fn compare mount_scope on_patch =
  if !(parent.disposed_scope) then
    invalid_arg "cannot create a keyed collection in a disposed scope";
  let scheduler = source.owner in
  let initial_items = sample source in
  let entries_ref = ref [] in
  reconcile_keyed scheduler entries_ref initial_items key_fn compare mount_scope
    on_patch;
  let disposed = ref false in
  let subscription =
    subscribe ~emit_initial:false source (fun items ->
        reconcile_keyed scheduler entries_ref items key_fn compare mount_scope
          on_patch)
  in
  let keyed_value =
    {
      keyed_subscription = subscription;
      keyed_entries = entries_ref;
      keyed_compare = compare;
      keyed_disposed = disposed;
    }
  in
  ignore
    (own parent { disposed; cancel = (fun () -> dispose_keyed keyed_value) });
  keyed_value

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
