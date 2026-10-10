open Signal

let require ok message = if not ok then failwith message
let failure f = match f () with () -> failwith "expected exception" | exception Failure message when message = "expected" -> ()
let invalid f = match f () with () -> failwith "expected Invalid_argument" | exception Invalid_argument _ -> ()

let notifications () =
  List.iter (fun before ->
    let owner = scheduler () in
    let input = state owner 0 and seen = ref [] in
    let bad () = subscribe ~emit_initial:false (value input) (fun _ -> failwith "expected") in
    let first = if before then Some (bad ()) else None in
    let derived = map (( * ) 2) (value input) in
    let last = if before then None else Some (bad ()) in
    ignore (subscribe ~emit_initial:false (value input) (fun v -> seen := v :: !seen));
    set input 1;
    failure (fun () -> stabilize owner);
    Option.iter dispose_subscription first; Option.iter dispose_subscription last;
    stabilize owner;
    require (get derived = 2 && !seen = [1]) "publication lost downstream notification";
    set input 2; stabilize owner;
    require (get derived = 4 && !seen = [2;1]) "publication failed to recover") [true;false]

let notification_cancellation () =
  let owner = scheduler () in
  let input = state owner 0 and later = ref None and calls = ref 0 in
  ignore (subscribe ~emit_initial:false (value input) (fun _ -> Option.iter dispose_subscription !later; failwith "expected"));
  later := Some (subscribe ~emit_initial:false (value input) (fun _ -> incr calls));
  set input 1; failure (fun () -> stabilize owner); stabilize owner;
  require (!calls = 0) "exception continuation ran cancelled subscriber"

let nested_stabilize () =
  List.iter (fun enqueue ->
    let owner = scheduler () and calls = ref [] in
    enqueue owner (fun () -> calls := 1 :: !calls; stabilize owner);
    enqueue owner (fun () -> calls := 2 :: !calls);
    (* The nested stabilize drains the snapshot synchronously: the second
       queued task runs inside the inner call instead of throwing or waiting
       for the outer run. Each call that ran work bumps the generation. *)
    stabilize owner;
    require (List.rev !calls = [1;2] && generation owner = 2) "nested stabilize did not drain queued work synchronously";
    enqueue owner (fun () -> stabilize owner; failwith "expected");
    enqueue owner (fun () -> calls := 3 :: !calls);
    failure (fun () -> stabilize owner); stabilize owner;
    require (List.rev !calls = [1;2;3]) "nested stabilize lost queued work after exception") [enqueue_effect;enqueue_dirty];
  let outer = scheduler () and inner = scheduler () and calls = ref 0 in
  enqueue_effect outer (fun () -> enqueue_effect inner (fun () -> incr calls); stabilize inner);
  stabilize outer; require (!calls = 1) "different scheduler nested flush blocked";
  let owner = scheduler () in
  let input = state owner 0 in
  let a = map (fun v -> if v > 0 then stabilize owner; v+1) (value input) in
  let b = map ((+) 2) (value input) in
  let joined = map2 (+) a b in
  set input 1; stabilize owner; require (get joined = 5) "derived nested flush changed dependency order"

let map2_construction () =
  List.iter (fun dead_on_left ->
    let owner = scheduler () in
    let dead = constant owner 0 and live = state owner 0 and calls = ref 0 in
    dispose_signal dead;
    let left,right = if dead_on_left then dead,value live else value live,dead in
    invalid (fun () -> ignore (map2 (fun a b -> incr calls; a+b) left right));
    let before = !calls in set live 1; stabilize owner;
    require (!calls = before) "rejected map2 retained an upstream subscription") [true;false];
  let owner = scheduler () in
  let left = state owner 0 and right = state owner 0 and calls = ref 0 in
  invalid (fun () -> ignore (map2 (fun a b -> incr calls; dispose_signal (value right); a+b) (value left) (value right)));
  set left 1; stabilize owner; require (!calls = 1) "map2 side effect during initial transform leaked registration"

let constructors () =
  List.iter (fun collection ->
    let owner = scheduler () and parent = scope "parent" and created = ref [] in
    let input = constant owner [1] in dispose_signal input;
    let mount _ = let sc = scope "root" in created := sc :: !created; sc in
    invalid (fun () -> if collection then ignore (keyed parent input Fun.id Int.compare mount ignore)
      else ignore (switch parent input (=) mount));
    dispose_scope parent;
    require (List.for_all (fun sc -> not (active sc)) !created) "failed constructor left active root scope") [true;false];
  List.iter (fun collection ->
    let owner = scheduler () and parent = scope "parent" and created = ref [] in
    let input = constant owner [1] in
    let mount _ = let sc = scope "root" in created := sc :: !created; on_mount sc (fun () -> failwith "expected"); sc in
    failure (fun () -> if collection then ignore (keyed parent input Fun.id Int.compare mount ignore)
      else ignore (switch parent input (=) mount));
    dispose_scope parent;
    require (List.for_all (fun sc -> not (active sc)) !created) "mount failure left active scope") [true;false]

let keyed_failures () =
  List.iter (fun phase ->
    let owner = scheduler () and parent = scope "parent" and made = ref [] and armed = ref false in
    let input = state owner [1;2] in
    let k = keyed parent (value input) Fun.id Int.compare
        (fun signal -> let key = get signal in let sc = scope "item" in made := sc :: !made;
          on_mount sc (fun () -> if !armed && phase=1 && key=4 then failwith "expected");
          on_unmount sc (fun () -> if !armed && phase=2 && key=2 then failwith "expected"); sc)
        (fun patch -> match patch with
          | Insert (4,_) when !armed && phase=0 -> failwith "expected"
          | Remove (1,_) when !armed && phase=3 -> failwith "expected"
          | _ -> ()) in
    armed := true; set input [3;4]; failure (fun () -> stabilize owner);
    armed := false; stabilize owner;
    require (List.for_all (fun e -> active (keyed_entry_scope e)) (keyed_entries k)) "failed reconcile retained disposed entry";
    set input [2]; stabilize owner;
    require (active (keyed_find_scope k 2)) "valid later edit reused disposed scope";
    dispose_scope parent;
    require (List.for_all (fun sc -> not (active sc)) !made) "partial reconcile orphaned created scopes") [0;1;2;3]

let lifecycle_disposal () =
  let owner = scheduler () and parent = scope "parent" and made = ref [] in
  let input = state owner 0 in
  let sw = switch parent (value input) (=) (fun key -> let sc = scope "branch" in made := sc :: !made;
      if key=0 then on_unmount sc (fun () -> dispose_scope parent); sc) in
  set input 1; stabilize owner; dispose_switch sw;
  require (List.for_all (fun sc -> not (active sc)) !made) "switch remounted after parent disposal";
  List.iter (fun phase ->
    let owner = scheduler () and parent = scope "parent" and made = ref [] and patches = ref 0 in
    let input = state owner [] in
    let k = keyed parent (value input) Fun.id Int.compare
        (fun signal -> let sc = scope "item" in made := sc :: !made;
          if phase=1 && get signal=2 then on_mount sc (fun () -> dispose_scope parent); sc)
        (fun patch -> incr patches; match patch with Insert (2,_) when phase=0 -> dispose_scope parent | _ -> ()) in
    set input [1;2;3]; stabilize owner; dispose_keyed k;
    require (List.for_all (fun sc -> not (active sc)) !made) "keyed retained in-flight scope after disposal";
    require (!patches <= 2) "keyed emitted patch after parent disposal") [0;1]

let mount_disposal () =
  let sc = scope "mount" and calls = ref 0 in
  on_mount sc (fun () -> dispose_scope sc);
  on_mount sc (fun () -> incr calls);
  mount sc; require (!calls = 0) "mount callback ran after scope disposal"

let ownership_churn () =
  let owner = scheduler () and parent = scope "owner" in
  let input = constant owner [1;2] in
  for _ = 1 to 256 do
    let sw = switch parent input (=) (fun _ -> scope "branch") in dispose_switch sw;
    let k = keyed parent input Fun.id Int.compare (fun _ -> scope "item") ignore in dispose_keyed k;
    require (keyed_entries k = []) "disposed collection retains item payloads"
  done;
  require (List.length (scope_owned_entries parent) <= 1) "owner retains history of disposed collection handles";
  dispose_scope parent

let switch_mount_retry () =
  let owner = scheduler () and parent = scope "parent" and armed = ref true in
  let input = state owner 0 in
  let sw = switch parent (value input) (=) (fun key -> let sc = scope "branch" in
      on_mount sc (fun () -> if key=1 && !armed then failwith "expected");
      on_unmount sc (fun () -> if key=1 && !armed then failwith "cleanup"); sc) in
  set input 1; failure (fun () -> stabilize owner);
  armed := false; set input 1; stabilize owner;
  require (active (switch_scope sw)) "same key failed to retry a failed mount";
  dispose_scope parent

let comparator_failure_after_remove () =
  let owner = scheduler () and parent = scope "parent" and armed = ref false in
  let input = state owner [1;2;3] in
  let k = keyed parent (value input) Fun.id
      (fun a b -> if !armed then failwith "expected" else Int.compare a b)
      (fun _ -> scope "item")
      (function Remove (1,_) -> armed := true | _ -> ()) in
  set input [2;3]; failure (fun () -> stabilize owner);
  armed := false; set input [1;2;3]; stabilize owner;
  require (active (keyed_find_scope k 1)) "comparator failure retained dead removed entry";
  dispose_scope parent

let public_reconcile_reentry () =
  let owner = scheduler () and entries = ref [] and made = ref [] in
  let mount _ = let sc = scope "item" in made := sc :: !made; sc in
  invalid (fun () -> reconcile_keyed owner entries [1] Fun.id Int.compare mount
      (fun _ -> reconcile_keyed owner entries [2] Fun.id Int.compare mount ignore));
  List.iter (fun entry -> dispose_scope (keyed_entry_scope entry)) !entries;
  require (List.for_all (fun sc -> not (active sc)) !made) "public reconcile reentry lost inner scope"

let switch_construction_reentry () =
  List.iter (fun phase ->
    let owner = scheduler () and parent = scope "parent" and made = ref [] in
    let input = state owner 0 in
    let sw = switch parent (value input) (=) (fun key ->
        let sc = scope (string_of_int key) in made := sc :: !made;
        let advance () = if key < 2 then (set input (key+1); stabilize owner) in
        if phase=0 then advance () else on_mount sc advance;
        sc) in
    require (scope_name (switch_scope sw) = "2") "switch missed publication during construction";
    require (active (switch_scope sw)) "switch construction returned inactive branch";
    dispose_scope parent;
    require (List.for_all scope_is_disposed !made) "construction reentry orphaned branch") [0;1];
  let parent = scope "parent" and owner = scheduler () and made = ref None in
  let sw = switch parent (constant owner 0) (=) (fun _ ->
      dispose_scope parent; let sc = scope "late" in made := Some sc; sc) in
  require (switch_is_disposed sw) "factory disposal did not cancel switch";
  require (match !made with Some sc -> scope_is_disposed sc | None -> false) "factory returned scope after parent disposal leaked"

let keyed_first_error () =
  let owner = scheduler () and parent = scope "parent" and armed = ref false in
  let input = state owner [1;2;3] in
  ignore (keyed parent (value input) Fun.id
      (fun a b -> if !armed then failwith "later comparator" else Int.compare a b)
      (fun _ -> scope "item")
      (function Remove (1,_) -> armed := true; failwith "first patch" | _ -> ()));
  set input [2;3];
  let message = try stabilize owner; "no exception" with Failure message -> message in
  require (message = "first patch") "later comparator replaced first callback exception";
  armed := false; dispose_scope parent

let switch_subscription_state () =
  let owner = scheduler () and parent = scope "parent" in
  let source = constant owner 0 in
  let sw = switch parent source (=) (fun _ -> scope "branch") in
  dispose_signal source;
  require (subscription_disposed (switch_subscription sw)) "switch subscription lost source cancellation state";
  dispose_scope parent

let registry_bounds () =
  let sc = scope "registry" and calls = ref [] in
  let handles = Array.init 64 (fun i -> register_cleanup sc (fun () -> calls := i :: !calls)) in
  for i=0 to 63 do
    let index = (i * 17) mod 64 in
    dispose_subscription handles.(index);
    require (scope_cleanup_count sc = 63-i) "cleanup live count drifted";
    require (List.length (scope_cleanup_entries sc) <= 2 * (63-i)) "cleanup tombstones exceed bound"
  done;
  dispose_scope sc;
  require (!calls = []) "cancelled callback ran after compaction";
  let parent = scope "owner" and owner = scheduler () in
  let source = constant owner 0 in
  let switches = Array.init 64 (fun _ -> switch parent source (=) (fun _ -> scope "branch")) in
  for i=0 to 63 do
    dispose_switch switches.((i * 17) mod 64);
    require (scope_owned_count parent = 63-i) "owner live count drifted";
    require (List.length (scope_owned_entries parent) <= 2 * (63-i)) "owner tombstones exceed bound"
  done;
  dispose_scope parent;
  let parent = scope "direct-cancel" in
  let sw = switch parent source (=) (fun _ -> scope "branch") in
  dispose_subscription (List.hd (scope_owned_entries parent));
  require (switch_is_disposed sw && not (active (switch_scope sw))) "owner handle cancellation leaked branch";
  dispose_scope parent

(* Review probes: two-phase stabilization keeps observers consistent. *)
let observer_consistent_graph () =
  let owner = scheduler () in
  let input = state owner 0 in
  let mid = map succ (value input) in
  let deep = map succ mid in
  let seen = ref [] in
  ignore (subscribe ~emit_initial:false mid (fun _ -> seen := get deep :: !seen));
  set input 1; stabilize owner;
  require (!seen = [3]) "observer read a mid-propagation value";
  (* A set staged by an observer lands in a later round of the same stabilize. *)
  let other = state owner 0 in
  let other_derived = map succ (value other) in
  ignore (subscribe ~emit_initial:false mid (fun _ -> set other 41));
  set input 3; stabilize owner;
  require (get other_derived = 42) "observer write did not stage a later round"

(* Review probes: a failed transform is rescheduled, not left stale. *)
let transform_reschedule () =
  let owner = scheduler () in
  let input = state owner 0 in
  let armed = ref true in
  let derived = map (fun v -> if !armed && v = 1 then failwith "expected"; v * 10) (value input) in
  set input 1;
  failure (fun () -> stabilize owner);
  armed := false;
  stabilize owner;
  require (get derived = 10) "failed transform did not recompute";
  set input 2; stabilize owner;
  require (get derived = 20) "transform stayed stale after recovery"

(* Review probes: a chain deeper than the round cap still completes —
   the cap measures real divergence, not rank depth. *)
let deep_chain_completes () =
  let owner = scheduler () in
  let input = state owner 0 in
  let last =
    List.fold_left (fun signal _ -> map succ signal) (value input)
      (List.init (max_stabilization_rounds + 1) Fun.id)
  in
  set input 1; stabilize owner;
  require (get last = max_stabilization_rounds + 2)
    "deep acyclic chain exceeded the round cap"

(* Review probes: own+cancel keeps the owned list bounded by the live set. *)
let own_cancel_bound () =
  let parent = scope "churn" and owner = scheduler () in
  let input = constant owner 0 in
  for _ = 1 to 10_000 do
    dispose_subscription (own parent (subscribe ~emit_initial:false input ignore))
  done;
  require (List.length (scope_owned_entries parent) < 1000)
    "owned handle list grows without bound";
  require (scope_owned_count parent = 0) "live owned count drifted";
  dispose_scope parent

(* Review probes: a derived whose subscribers went away stops recomputing and
   reconnects on demand. *)
let necessity_release () =
  let owner = scheduler () in
  let input = state owner 0 in
  let calls = ref 0 in
  let derived = map (fun v -> incr calls; v) (value input) in
  let sub = subscribe ~emit_initial:false derived ignore in
  set input 1; stabilize owner;
  dispose_subscription sub;
  let live_calls = !calls in
  set input 2; stabilize owner;
  require (!calls = live_calls) "orphaned derived kept recomputing";
  let seen = ref [] in
  let _sub = subscribe ~emit_initial:false derived (fun v -> seen := v :: !seen) in
  set input 3; stabilize owner;
  require (!seen = [3] && get derived = 3) "resubscribed derived missed a publication"

let tests = ["notifications",notifications; "notification cancellation",notification_cancellation;
  "nested stabilize",nested_stabilize; "map2 construction",map2_construction;
  "constructors",constructors; "keyed failures",keyed_failures;
  "lifecycle disposal",lifecycle_disposal; "mount disposal",mount_disposal;
  "ownership churn",ownership_churn; "switch mount retry",switch_mount_retry;
  "comparator failure after remove",comparator_failure_after_remove;
  "public reconcile reentry",public_reconcile_reentry;
  "switch construction reentry",switch_construction_reentry;
  "keyed first error",keyed_first_error;
  "switch subscription state",switch_subscription_state;
  "registry bounds",registry_bounds;
  "observer consistent graph",observer_consistent_graph;
  "transform reschedule",transform_reschedule;
  "deep chain completes",deep_chain_completes;
  "own cancel bound",own_cancel_bound;
  "necessity release",necessity_release]

let () =
  let failures = ref 0 in
  List.iter (fun (name,f) -> try f (); Printf.printf "PASS %s\n%!" name
    with exn -> incr failures; Printf.printf "FAIL %s: %s\n%!" name (Printexc.to_string exn)) tests;
  if !failures <> 0 then exit 1
