open Signal

type test_item = { key : string; item_value : int }

let item key item_value = { key; item_value }

let check = Alcotest.(check (list string))
let check_int = Alcotest.(check int)
let check_bool = Alcotest.(check bool)

let raises_invalid_arg f =
  match f () with
  | _ -> false
  | exception Invalid_argument _ -> true

let test_constant_and_state_batching () =
  let scheduler = scheduler () in
  let constant = constant scheduler "ready" in
  let state = state scheduler 0 in
  let observed = ref [] in
  let _subscription =
    observe (value state) (fun v -> observed := !(observed) @ [ string_of_int v ])
  in
  Alcotest.(check string) "constant value" "ready" (sample constant);
  check "observer receives mounted value" [ "0" ] !observed;
  set state 1;
  set state 2;
  update state (fun v -> v + 3);
  check_int "state remains stable before stabilization" 0 (get_state state);
  check_int "initial generation" 0 (generation scheduler);
  stabilize scheduler;
  check_int "batched state result" 5 (get_state state);
  check "batched state emits once" [ "0"; "5" ] !observed;
  check_int "generation after work" 1 (generation scheduler);
  stabilize scheduler;
  check_int "no-op stabilization" 1 (generation scheduler)

let test_incremental_map () =
  let scheduler = scheduler () in
  let left = state scheduler 1 in
  let right = state scheduler "x" in
  let unary_calls = ref 0 in
  let doubled =
    map
      (fun v ->
        incr unary_calls;
        v * 2)
      (value left)
  in
  let binary_calls = ref 0 in
  let combined =
    map2
      (fun number text ->
        incr binary_calls;
        string_of_int number ^ ":" ^ text)
      (value left) (value right)
  in
  check_int "initial unary map" 2 (sample doubled);
  Alcotest.(check string) "initial binary map" "1:x" (sample combined);
  check_int "unary map initializes once" 1 !unary_calls;
  check_int "binary map initializes once" 1 !binary_calls;
  set left 2;
  set left 3;
  set right "y";
  stabilize scheduler;
  check_int "unary map stabilized value" 6 (sample doubled);
  Alcotest.(check string) "binary map stabilized value" "3:y" (sample combined);
  check_int "unary map recomputes once per batch" 2 !unary_calls;
  check_int "binary map recomputes once per batch" 2 !binary_calls

let test_stabilization_diagnostics_count_scheduled_work () =
  let scheduler = scheduler () in
  let source = state scheduler 1 in
  let derived = map (fun v -> v * 2) (value source) in
  let observed = ref [] in
  let _subscription =
    observe derived (fun v -> observed := !(observed) @ [ string_of_int v ])
  in
  set source 2;
  set source 3;
  stabilize scheduler;
  let diagnostics = last_stabilization scheduler in
  check_int "work advances one scheduler generation" 1
    diagnostics.stabilization_generation;
  check_int "state publication and derived recomputation use two rounds" 2
    diagnostics.stabilization_rounds;
  check_int "the update schedules no effects" 0 diagnostics.stabilization_effects;
  check_int "batched writes publish once and recompute once" 2
    diagnostics.stabilization_dirty_tasks;
  check "the derived observer runs only once" [ "2"; "6" ] !observed;
  stabilize scheduler;
  let diagnostics = last_stabilization scheduler in
  check_int "a no-op keeps the scheduler generation" 1
    diagnostics.stabilization_generation;
  check_int "a no-op reports zero rounds" 0 diagnostics.stabilization_rounds;
  check_int "a no-op reports zero effects" 0 diagnostics.stabilization_effects;
  check_int "a no-op reports zero dirty work" 0
    diagnostics.stabilization_dirty_tasks

let test_map_rejects_mixed_schedulers () =
  let left = constant (scheduler ()) 1 in
  let right = constant (scheduler ()) 2 in
  let rejected =
    raises_invalid_arg (fun () -> ignore (map2 ( + ) left right))
  in
  check_bool "map inputs must share one scheduler" true rejected

let test_cutoff_and_subscription_cleanup () =
  let scheduler = scheduler () in
  let state = state scheduler 10 in
  let by_decade =
    cutoff (fun left right -> left / 10 = right / 10) (value state)
  in
  let observed = ref [] in
  let subscription =
    observe by_decade (fun v -> observed := !(observed) @ [ string_of_int v ])
  in
  set state 11;
  stabilize scheduler;
  check "cutoff suppresses equivalent changes" [ "10" ] !observed;
  set state 20;
  stabilize scheduler;
  check "cutoff forwards meaningful changes" [ "10"; "20" ] !observed;
  dispose_subscription subscription;
  dispose_subscription subscription;
  set state 30;
  stabilize scheduler;
  check "disposed observer stays detached" [ "10"; "20" ] !observed

let test_derived_signal_disposal () =
  let scheduler = scheduler () in
  let source = state scheduler 1 in
  let calls = ref 0 in
  let derived =
    map
      (fun current ->
        incr calls;
        current * 2)
      (value source)
  in
  check_int "derived signal initializes" 2 (sample derived);
  check_int "transform runs for initialization" 1 !calls;
  dispose_signal derived;
  dispose_signal derived;
  set source 2;
  stabilize scheduler;
  check_int "disposed signal keeps its final value" 2 (sample derived);
  check_int "disposed signal detaches from its source" 1 !calls

let test_scope_owned_signal_disposal () =
  let scheduler = scheduler () in
  let scope = scope "derived" in
  let source = state scheduler 1 in
  let calls = ref 0 in
  let derived =
    own_signal scope
      (map
         (fun current ->
           incr calls;
           current * 2)
         (value source))
  in
  check_int "owned signal initializes" 2 (sample derived);
  dispose_scope scope;
  set source 2;
  stabilize scheduler;
  check_int "scope disposal detaches owned signal" 1 !calls

let test_effect_queue () =
  let scheduler = scheduler () in
  let state = state scheduler 0 in
  let trace = ref [] in
  enqueue_effect scheduler (fun () ->
      trace := !trace @ [ "first" ];
      set state 1;
      enqueue_effect scheduler (fun () ->
          trace := !trace @ [ "third" ];
          update state (fun v -> v + 10)));
  enqueue_effect scheduler (fun () ->
      trace := !trace @ [ "second" ];
      update state (fun v -> v + 1));
  stabilize scheduler;
  check "effects execute in FIFO order" [ "first"; "second"; "third" ] !trace;
  check_int "effects share one stabilization batch" 12 (get_state state);
  check_int "effect batch generation" 1 (generation scheduler)

let test_scope_lifecycle_and_state_slots () =
  let scheduler = scheduler () in
  let trace = ref [] in
  let parent = scope "parent" in
  let child = child_scope "child" parent in
  let slot = state_slot "count" in
  on_mount parent (fun () -> trace := !trace @ [ "mount-parent" ]);
  on_unmount parent (fun () -> trace := !trace @ [ "unmount-parent" ]);
  on_mount child (fun () -> trace := !trace @ [ "mount-child" ]);
  on_unmount child (fun () -> trace := !trace @ [ "unmount-child" ]);
  mount parent;
  mount child;
  let first_state = state_at scheduler parent slot 7 in
  set first_state 9;
  stabilize scheduler;
  let second_state = state_at scheduler parent slot 999 in
  check_bool "state slot preserves identity" true (first_state == second_state);
  check_int "reused state slot ignores new initializer" 9 (get_state second_state);
  dispose_scope parent;
  dispose_scope parent;
  check_int "scope disposal releases state slots" 0
    (Hashtbl.length slot.slot_states);
  check_bool "disposed parent is inactive" false (active parent);
  check_bool "disposing parent disposes child" false (active child);
  check "scope lifecycle order"
    [ "mount-parent"; "mount-child"; "unmount-child"; "unmount-parent" ]
    !trace

let test_scope_disposal_boundaries () =
  let scheduler = scheduler () in
  let parent = scope "parent" in
  let slot = state_slot "state" in
  let cleanup_calls = ref 0 in
  on_dispose parent (fun () ->
      incr cleanup_calls;
      dispose_scope parent);
  dispose_scope parent;
  check_int "scope disposal is reentrant-safe" 1 !cleanup_calls;
  let child_rejected =
    raises_invalid_arg (fun () -> ignore (child_scope "late-child" parent))
  in
  let state_rejected =
    raises_invalid_arg (fun () -> ignore (state_at scheduler parent slot 0))
  in
  check_bool "disposed scope rejects new children" true child_rejected;
  check_bool "disposed scope rejects new state" true state_rejected

let test_switch_lifecycle () =
  let scheduler = scheduler () in
  let parent = scope "screen" in
  let selected = state scheduler false in
  let trace = ref [] in
  mount parent;
  let switch =
    switch parent (value selected) ( = ) (fun key ->
        let name = if key then "content" else "loading" in
        let branch = child_scope name parent in
        on_mount branch (fun () -> trace := !trace @ [ "mount-" ^ name ]);
        on_unmount branch (fun () -> trace := !trace @ [ "unmount-" ^ name ]);
        branch)
  in
  check "initial switch branch" [ "mount-loading" ] !trace;
  check_int "parent owns only the active switch branch" 1
    (List.length !(parent.cleanup_callbacks));
  set selected false;
  stabilize scheduler;
  check "equal key does not remount" [ "mount-loading" ] !trace;
  set selected true;
  stabilize scheduler;
  check "switch replaces only its branch"
    [ "mount-loading"; "unmount-loading"; "mount-content" ]
    !trace;
  check_int "replaced branch detaches from its parent" 1
    (List.length !(parent.cleanup_callbacks));
  dispose_switch switch;
  check_int "disposed switch leaves no retained branch" 0
    (List.length !(parent.cleanup_callbacks));
  check "switch disposal unmounts active branch"
    [ "mount-loading"; "unmount-loading"; "mount-content"; "unmount-content" ]
    !trace

let test_keyed_collection () =
  let scheduler = scheduler () in
  let parent = scope "list" in
  let items = state scheduler [ item "a" 1; item "b" 2; item "c" 3 ] in
  let patches = ref [] in
  let updates = ref [] in
  let unmounted = ref [] in
  mount parent;
  let keyed =
    keyed parent (value items)
      (fun i -> i.key)
      (fun left right -> Stdlib.compare left right)
      (fun current_signal ->
        let key = (sample current_signal).key in
        let child = child_scope key parent in
        ignore
          (own child
             (observe current_signal (fun current ->
                  updates :=
                    !updates
                    @ [ current.key ^ ":" ^ string_of_int current.item_value ])));
        on_unmount child (fun () -> unmounted := !unmounted @ [ key ]);
        child)
      (fun patch -> patches := !patches @ [ patch ])
  in
  check_int "initial keyed insert count" 3 (List.length !patches);
  check_int "parent owns the visible keyed scopes" 3
    (List.length !(parent.cleanup_callbacks));
  check "keyed items expose initial values" [ "a:1"; "b:2"; "c:3" ] !updates;
  (match List.nth !patches 0 with
   | Insert (key, index) ->
     Alcotest.(check string) "first inserted key" "a" key;
     check_int "first inserted index" 0 index
   | _ -> Alcotest.fail "first patch is Insert");
  (match List.nth !patches 1 with
   | Insert (key, index) ->
     Alcotest.(check string) "second inserted key" "b" key;
     check_int "second inserted index" 1 index
   | _ -> Alcotest.fail "second patch is Insert");
  (match List.nth !patches 2 with
   | Insert (key, index) ->
     Alcotest.(check string) "third inserted key" "c" key;
     check_int "third inserted index" 2 index
   | _ -> Alcotest.fail "third patch is Insert");
  let original_a = keyed_find_scope keyed "a" in
  patches := [];
  set items [ item "c" 30; item "a" 10; item "b" 20 ];
  stabilize scheduler;
  check_int "keyed reorder patch count" 1 (List.length !patches);
  (match List.nth !patches 0 with
   | Move (key, from_index, to_index) ->
     Alcotest.(check string) "moved key" "c" key;
     check_int "move source index" 2 from_index;
     check_int "move target index" 0 to_index
   | _ -> Alcotest.fail "reorder patch is Move");
  check_bool "keyed move preserves scope identity" true
    (original_a == keyed_find_scope keyed "a");
  check "keyed scopes receive changed item values"
    [ "a:1"; "b:2"; "c:3"; "c:30"; "a:10"; "b:20" ]
    !updates;
  patches := [];
  set items [ item "c" 30; item "d" 40; item "a" 10 ];
  stabilize scheduler;
  check_int "keyed update patch count" 2 (List.length !patches);
  check_int "removed keyed scopes detach from their parent" 3
    (List.length !(parent.cleanup_callbacks));
  (match List.nth !patches 0 with
   | Remove (key, index) ->
     Alcotest.(check string) "removed key" "b" key;
     check_int "removed index" 2 index
   | _ -> Alcotest.fail "first keyed update patch is Remove");
  (match List.nth !patches 1 with
   | Insert (key, index) ->
     Alcotest.(check string) "inserted key" "d" key;
     check_int "inserted index" 1 index
   | _ -> Alcotest.fail "second keyed update patch is Insert");
  check "removed scope is disposed" [ "b" ] !unmounted;
  dispose_keyed keyed;
  check_int "keyed disposal leaves no retained child scopes" 0
    (List.length !(parent.cleanup_callbacks));
  check "keyed disposal follows visible order" [ "b"; "c"; "d"; "a" ] !unmounted

let test_keyed_duplicates () =
  let scheduler = scheduler () in
  let parent = scope "list" in
  let items = state scheduler [ item "a" 1 ] in
  mount parent;
  let keyed =
    keyed parent (value items)
      (fun i -> i.key)
      (fun left right -> Stdlib.compare left right)
      (fun current -> child_scope (sample current).key parent)
      (fun _patch -> ())
  in
  set items [ item "a" 1; item "a" 2 ];
  let rejected = raises_invalid_arg (fun () -> stabilize scheduler) in
  check_bool "duplicate keyed items are rejected" true rejected;
  dispose_keyed keyed

let test_keyed_lookup_uses_comparator () =
  let scheduler = scheduler () in
  let parent = scope "lookup" in
  let items = constant scheduler [ item "a" 1 ] in
  let keyed =
    keyed parent items
      (fun i -> i.key)
      (fun left right -> Stdlib.compare (String.length left) (String.length right))
      (fun current -> child_scope (sample current).key parent)
      (fun _patch -> ())
  in
  check_bool "keyed lookup uses the configured comparator" true
    (keyed_find_scope keyed "a" == keyed_find_scope keyed "z");
  dispose_keyed keyed

(* Additional coverage for API paths not exercised by the ported suite *)

let test_observe_rejects_disposed_signal () =
  let scheduler = scheduler () in
  let constant = constant scheduler 1 in
  dispose_signal constant;
  let rejected =
    raises_invalid_arg (fun () -> ignore (observe constant (fun _ -> ())))
  in
  check_bool "cannot observe a disposed signal" true rejected

let test_subscribe_without_initial_emit () =
  let scheduler = scheduler () in
  let source = state scheduler 1 in
  let seen = ref [] in
  let _subscription =
    subscribe ~emit_initial:false (value source) (fun v ->
        seen := !seen @ [ v ])
  in
  Alcotest.(check (list int)) "no initial emit" [] !seen;
  set source 2;
  stabilize scheduler;
  Alcotest.(check (list int)) "subscriber sees updates" [ 2 ] !seen

let test_register_cleanup_cancel () =
  let scope = scope "scoped" in
  let calls = ref 0 in
  let subscription = register_cleanup scope (fun () -> incr calls) in
  dispose_subscription subscription;
  dispose_scope scope;
  check_int "cancelled cleanup does not run" 0 !calls

let test_own_on_disposed_scope_disposes_subscription () =
  let scheduler = scheduler () in
  let scope = scope "gone" in
  let source = state scheduler 1 in
  let seen = ref [] in
  let subscription =
    subscribe ~emit_initial:false (value source) (fun v ->
        seen := !seen @ [ v ])
  in
  dispose_scope scope;
  let _ = own scope subscription in
  set source 2;
  stabilize scheduler;
  Alcotest.(check (list int)) "owning on a disposed scope disposes" [] !seen

let test_keyed_find_scope_missing_raises () =
  let scheduler = scheduler () in
  let parent = scope "list" in
  let items = constant scheduler [ item "a" 1 ] in
  let keyed =
    keyed parent items
      (fun i -> i.key)
      (fun left right -> Stdlib.compare left right)
      (fun current -> child_scope (sample current).key parent)
      (fun _patch -> ())
  in
  let rejected =
    raises_invalid_arg (fun () -> ignore (keyed_find_scope keyed "zz"))
  in
  check_bool "missing keyed scope raises" true rejected;
  dispose_keyed keyed

let () =
  Alcotest.run "ocaml-signal"
    [
      ( "signal",
        [
          Alcotest.test_case "constant and state batching" `Quick
            test_constant_and_state_batching;
          Alcotest.test_case "incremental map" `Quick test_incremental_map;
          Alcotest.test_case "stabilization diagnostics count scheduled work"
            `Quick test_stabilization_diagnostics_count_scheduled_work;
          Alcotest.test_case "map rejects mixed schedulers" `Quick
            test_map_rejects_mixed_schedulers;
          Alcotest.test_case "cutoff and subscription cleanup" `Quick
            test_cutoff_and_subscription_cleanup;
          Alcotest.test_case "derived signal disposal" `Quick
            test_derived_signal_disposal;
          Alcotest.test_case "scope owned signal disposal" `Quick
            test_scope_owned_signal_disposal;
          Alcotest.test_case "effect queue" `Quick test_effect_queue;
          Alcotest.test_case "scope lifecycle and state slots" `Quick
            test_scope_lifecycle_and_state_slots;
          Alcotest.test_case "scope disposal boundaries" `Quick
            test_scope_disposal_boundaries;
          Alcotest.test_case "switch lifecycle" `Quick test_switch_lifecycle;
          Alcotest.test_case "keyed collection" `Quick test_keyed_collection;
          Alcotest.test_case "keyed duplicates" `Quick test_keyed_duplicates;
          Alcotest.test_case "keyed lookup uses comparator" `Quick
            test_keyed_lookup_uses_comparator;
          Alcotest.test_case "observe rejects disposed signal" `Quick
            test_observe_rejects_disposed_signal;
          Alcotest.test_case "subscribe without initial emit" `Quick
            test_subscribe_without_initial_emit;
          Alcotest.test_case "register cleanup cancel" `Quick
            test_register_cleanup_cancel;
          Alcotest.test_case "own on disposed scope disposes subscription"
            `Quick test_own_on_disposed_scope_disposes_subscription;
          Alcotest.test_case "keyed find scope missing raises" `Quick
            test_keyed_find_scope_missing_raises;
        ] );
    ]
