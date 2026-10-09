type stabilization_diagnostics = {
  stabilization_generation : int;
  stabilization_rounds : int;
  stabilization_effects : int;
  stabilization_dirty_tasks : int;
}

type scheduler = {
  effects : (unit -> unit) list ref;
  dirty : (unit -> unit) list ref;
  computations : (int * (unit -> unit)) list ref;
  generation_value : int ref;
  last_stabilization_value : stabilization_diagnostics ref;
}

type subscription = { disposed : bool ref; cancel : unit -> unit }

type 'value subscriber = { subscriber_id : int; callback : 'value -> unit }

type cleanup_entry = { cleanup_id : int; cleanup_callback : unit -> unit }

type 'value signal = {
  owner : scheduler;
  rank : int;
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

let scheduler () =
  {
    effects = ref [];
    dirty = ref [];
    computations = ref [];
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

let enqueue_effect owner f = owner.effects := !(owner.effects) @ [ f ]

let enqueue_dirty owner task = owner.dirty := !(owner.dirty) @ [ task ]

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
    reactive.owner.computations :=
      !(reactive.owner.computations)
      @ [
          ( reactive.rank,
            fun () ->
              scheduled := false;
              if not !(reactive.disposed_signal) then task () );
        ]
  end

let rec run_tasks queue run = function
  | [] -> ()
  | task :: remaining ->
      (try run task
       with exn ->
         queue := remaining @ !queue;
         raise exn);
      run_tasks queue run remaining

let max_stabilization_rounds = 10000

exception Stabilization_limit_exceeded of int * int * int

let stabilize owner =
  let worked = ref false in
  let rounds = ref 0 in
  let effect_count = ref 0 in
  let dirty_count = ref 0 in
  let continue = ref true in
  while !continue do
    let effects = !(owner.effects) in
    let dirty = !(owner.dirty) in
    if effects = [] && dirty = [] && !(owner.computations) = [] then
      continue := false
    else begin
      worked := true;
      incr rounds;
      if !rounds > max_stabilization_rounds then
        raise
          (Stabilization_limit_exceeded
             (max_stabilization_rounds, !effect_count, !dirty_count));
      owner.effects := [];
      run_tasks owner.effects
        (fun f ->
          incr effect_count;
          f ())
        effects;
      let pending_dirty = !(owner.dirty) in
      owner.dirty := [];
      run_tasks owner.dirty
        (fun task ->
          incr dirty_count;
          task ())
        pending_dirty;
      if effects = [] && pending_dirty = [] && !(owner.computations) <> [] then begin
        (* Settle mutations first, then recompute one dependency rank per round. *)
        let rank =
          List.fold_left
            (fun lowest (rank, _) -> min lowest rank)
            max_int !(owner.computations)
        in
        let ready, waiting =
          List.partition
            (fun (task_rank, _) -> task_rank = rank)
            !(owner.computations)
        in
        owner.computations := waiting;
        run_tasks owner.computations
          (fun (_, task) ->
            incr dirty_count;
            task ())
          ready
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
    subscribers = ref [];
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
    { subscriber_id; callback = (fun v -> if not !disposed then callback v) }
  in
  let cancel () =
    if !disposed then ()
    else begin
      disposed := true;
      reactive.subscribers :=
        List.filter
          (fun current -> current.subscriber_id <> subscriber_id)
          !(reactive.subscribers)
    end
  in
  reactive.subscribers := !(reactive.subscribers) @ [ subscriber_value ];
  if emit_initial then callback (sample reactive);
  { disposed; cancel }

let observe reactive callback = subscribe ~emit_initial:true reactive callback

let dispose_subscription subscription = subscription.cancel ()

let dispose_signal reactive =
  if !(reactive.disposed_signal)
  then ()
  else begin
    reactive.disposed_signal := true;
    List.iter dispose_subscription !(reactive.upstream_subscriptions);
    reactive.upstream_subscriptions := [];
    reactive.subscribers := []
  end

let publish reactive next_value =
  if not !(reactive.disposed_signal) then begin
    reactive.current := next_value;
    List.iter
      (fun subscriber_value ->
        if not !(reactive.disposed_signal) then
          subscriber_value.callback next_value)
      !(reactive.subscribers)
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
  if !(scope_value.disposed_scope)
  then begin
    callback ();
    { disposed = ref true; cancel = (fun () -> ()) }
  end
  else begin
    incr scope_value.next_cleanup_id;
    let cleanup_id = !(scope_value.next_cleanup_id) in
    let disposed = ref false in
    let entry = { cleanup_id; cleanup_callback = callback } in
    let cancel () =
      if !disposed
      then ()
      else begin
        disposed := true;
        scope_value.cleanup_callbacks :=
          List.filter
            (fun current -> current.cleanup_id <> cleanup_id)
            !(scope_value.cleanup_callbacks)
      end
    in
    scope_value.cleanup_callbacks :=
      !(scope_value.cleanup_callbacks) @ [ entry ];
    { disposed; cancel }
  end

let rec child_scope name parent =
  if !(parent.disposed_scope)
  then invalid_arg "cannot create a child of a disposed scope";
  let child = make_scope name in
  ignore
    (own child (register_cleanup parent (fun () -> dispose_scope child)));
  child

and own scope_value subscription =
  if !(scope_value.disposed_scope)
  then dispose_subscription subscription
  else
    scope_value.owned_subscriptions :=
      !(scope_value.owned_subscriptions) @ [ subscription ];
  subscription

and dispose_scope scope_value =
  if !(scope_value.disposed_scope)
  then ()
  else begin
    scope_value.disposed_scope := true;
    List.iter
      (fun cleanup -> cleanup.cleanup_callback ())
      !(scope_value.cleanup_callbacks);
    List.iter dispose_subscription !(scope_value.owned_subscriptions);
    if !(scope_value.mounted)
    then
      List.iter (fun callback -> callback ()) !(scope_value.unmount_callbacks);
    scope_value.mounted := false;
    scope_value.cleanup_callbacks := [];
    scope_value.owned_subscriptions := []
  end

let scope name = make_scope name

let on_mount scope_value callback =
  if !(scope_value.disposed_scope) then ()
  else if !(scope_value.mounted) then callback ()
  else
    scope_value.mount_callbacks := !(scope_value.mount_callbacks) @ [ callback ]

let on_unmount scope_value callback =
  scope_value.unmount_callbacks :=
    !(scope_value.unmount_callbacks) @ [ callback ]

let on_dispose scope_value callback =
  ignore (register_cleanup scope_value callback)

let own_signal scope_value reactive =
  on_dispose scope_value (fun () -> dispose_signal reactive);
  reactive

let mount scope_value =
  if !(scope_value.disposed_scope) || !(scope_value.mounted)
  then ()
  else begin
    scope_value.mounted := true;
    List.iter (fun callback -> callback ()) !(scope_value.mount_callbacks)
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

let reconcile_keyed scheduler entries_ref items key_fn compare mount_scope
    on_patch =
  let new_index = key_index items key_fn compare in
  let without_removed =
    let rec loop index entries =
      if index < 0 then entries
      else
        let entry = List.nth entries index in
        let key = entry.entry_key in
        match new_index key with
        | Some _ -> loop (index - 1) entries
        | None ->
            on_patch (Remove (key, index));
            dispose_scope entry.entry_scope;
            loop (index - 1) (remove_entry_at entries index)
    in
    loop (List.length !entries_ref - 1) !entries_ref
  in
  let items = Array.of_list items in
  let nth_opt entries index =
    if index < List.length entries then Some (List.nth entries index) else None
  in
  let reconciled =
    let rec loop index entries =
      if index = Array.length items then entries
      else
        let current = items.(index) in
        let key = key_fn current in
        match nth_opt entries index with
        | Some entry when compare entry.entry_key key = 0 ->
            set_keyed_item entry.entry_state current;
            loop (index + 1) entries
        | _ -> (
            match find_entry_index entries key compare with
            | Some existing_index ->
                let entry = List.nth entries existing_index in
                set_keyed_item entry.entry_state current;
                on_patch (Move (key, existing_index, index));
                loop (index + 1) (move_entry entries existing_index index)
            | None ->
                let item_state = state scheduler current in
                let child = mount_scope (value item_state) in
                let entry =
                  {
                    entry_key = key;
                    entry_state = item_state;
                    entry_scope = child;
                  }
                in
                mount child;
                on_patch (Insert (key, index));
                loop (index + 1) (insert_entry_at entries index entry))
    in
    loop 0 without_removed
  in
  entries_ref := reconciled

let dispose_keyed keyed_value =
  if !(keyed_value.keyed_disposed) then ()
  else begin
    keyed_value.keyed_disposed := true;
    dispose_subscription keyed_value.keyed_subscription;
    List.iter
      (fun entry -> dispose_scope entry.entry_scope)
      !(keyed_value.keyed_entries)
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
