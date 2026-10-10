open Signal

let require condition message = if not condition then failwith message

let cancelled_callback () =
  let parent = scope "retained owner" in
  (* Keep two live registrations, so cancelling one entry leaves a tombstone. *)
  ignore (register_cleanup parent ignore);
  ignore (register_cleanup parent ignore);
  let weak = Weak.create 1 in
  let handle =
    let payload = Bytes.make 1_000_000 'x' in
    Weak.set weak 0 (Some payload);
    register_cleanup parent (fun () -> require (Bytes.length payload > 0) "payload")
  in
  let retained_entry = List.hd (scope_cleanup_entries parent) in
  dispose_subscription handle;
  Gc.full_major ();
  require (not (Weak.check weak 0)) "cancelled cleanup retains captured payload";
  require (List.length (scope_cleanup_entries parent) = 3) "probe did not retain a tombstone";
  cleanup_entry_run retained_entry;
  dispose_subscription handle;
  dispose_scope parent

let cancelled_collections () =
  let owner = scheduler () and parent = scope "retained owner" in
  let source = constant owner [1] in
  let active_switch = switch parent source (=) (fun _ -> scope "active") in
  let active_keyed = keyed parent source Fun.id Int.compare (fun _ -> scope "active") ignore in
  let weak = Weak.create 2 in
  (let sw = switch parent source (=) (fun _ -> scope "cancelled") in
   Weak.set weak 0 (Some (Obj.repr sw)); dispose_switch sw);
  (let k = keyed parent source Fun.id Int.compare (fun _ -> scope "cancelled") ignore in
   Weak.set weak 1 (Some (Obj.repr k)); dispose_keyed k);
  Gc.full_major ();
  require (not (Weak.check weak 0) && not (Weak.check weak 1))
    "owner tombstones retain disposed collection records";
  dispose_switch active_switch;
  dispose_keyed active_keyed;
  dispose_scope parent

let registry_peak_storage () =
  List.iter (fun mode ->
    let sc = scope "registry peak" in
    let before = Obj.reachable_words (Obj.repr sc) in
    let handles = List.init 10_000 (fun _ -> register_cleanup sc ignore) in
    (match mode with
     | 0 -> List.iter dispose_subscription handles
     | 1 -> dispose_scope sc
     | _ -> List.iteri (fun i handle -> if i >= 2 then dispose_subscription handle) handles);
    let retained = Obj.reachable_words (Obj.repr sc) in
    require (retained <= before + 512) "scope retains peak hash bucket storage";
    dispose_scope sc) [0;1;2]

let () =
  cancelled_callback ();
  cancelled_collections ();
  registry_peak_storage ();
  print_endline "PASS cancelled callback payloads and collection records are collectible"
