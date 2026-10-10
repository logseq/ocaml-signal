open Signal
module Q = QCheck2
module G = Q.Gen

let require condition message = if not condition then Q.Test.fail_report message

let int_equal label expected actual =
  if expected <> actual then
    Q.Test.fail_reportf "%s: expected %d, got %d" label expected actual

let bounded_list maximum gen = G.list_size (G.int_range 0 maximum) gen
let integers = G.int_range (-100) 100

let property ?(count = 500) name ~print gen f =
  Q.Test.make ~name ~count ~long_factor:10 ~print gen (fun input ->
      f input;
      true)

type operation = Write of int | Add of int | Flush

let operation_gen =
  G.oneof
    [
      G.map (fun n -> Write n) integers;
      G.map (fun n -> Add n) integers;
      G.return Flush;
    ]

let show_operation = function
  | Write n -> Printf.sprintf "set %d" n
  | Add n -> Printf.sprintf "add %d" n
  | Flush -> "flush"

let batching =
  property "state operations agree with a staged reference model"
    ~print:(Q.Print.list show_operation) (bounded_list 100 operation_gen)
    (fun operations ->
      let owner = scheduler () in
      let input = state owner 0 in
      let committed = ref 0 and pending = ref None and seen = ref [] in
      ignore
        (subscribe ~emit_initial:false (value input) (fun n ->
             seen := n :: !seen));
      let flush () =
        seen := [];
        let expected =
          match !pending with
          | None -> []
          | Some n ->
              committed := n;
              [ n ]
        in
        stabilize owner;
        require (!seen = expected)
          "publication must match the last staged write";
        int_equal "committed state" !committed (get_state input);
        int_equal "one task per dirty state" (List.length expected)
          (last_stabilization owner).stabilization_dirty_tasks;
        pending := None
      in
      List.iter
        (function
          | Write n ->
              set input n;
              pending := Some n;
              int_equal "staged writes are invisible" !committed
                (get_state input)
          | Add n ->
              update input (( + ) n);
              pending := Some (Option.value !pending ~default:!committed + n)
          | Flush -> flush ())
        operations;
      flush ())

type node = Unary of int * int | Binary of int * int | Cutoff of int

let node_gen =
  G.oneof
    [
      G.map2
        (fun source offset -> Unary (source, offset))
        (G.int_range 0 100) integers;
      G.map2
        (fun left right -> Binary (left, right))
        (G.int_range 0 100) (G.int_range 0 100);
      G.map (fun source -> Cutoff source) (G.int_range 0 100);
    ]

let show_node = function
  | Unary (i, n) -> Printf.sprintf "map(%d,%d)" i n
  | Binary (i, j) -> Printf.sprintf "map2(%d,%d)" i j
  | Cutoff i -> Printf.sprintf "cutoff(%d)" i

let write_gen = G.pair (G.int_range 0 3) integers

let graph_gen =
  G.pair (bounded_list 40 node_gen)
    (G.list_size (G.int_range 1 12) (bounded_list 20 write_gen))

let graphs =
  property "random DAGs settle once per affected node without glitches"
    ~print:
      (Q.Print.pair (Q.Print.list show_node)
         (Q.Print.list (Q.Print.list (Q.Print.pair Q.Print.int Q.Print.int))))
    graph_gen
    (fun (definitions, batches) ->
      let owner = scheduler () in
      let roots = Array.init 4 (fun _ -> state owner 0) in
      let size = 4 + List.length definitions in
      let signals = Array.make size (value roots.(0)) in
      Array.iteri (fun i root -> signals.(i) <- value root) roots;
      let expected = Array.make size 0 and calls = Array.make size 0 in
      let nodes = Array.of_list definitions in
      Array.iteri
        (fun offset node ->
          let index = offset + 4 in
          let source n = n mod index in
          let count f =
            calls.(index) <- calls.(index) + 1;
            f ()
          in
          let signal =
            match node with
            | Unary (n, amount) ->
                map
                  (fun v -> count (fun () -> (v + amount) mod 1009))
                  signals.(source n)
            | Binary (a, b) ->
                map2
                  (fun a b -> count (fun () -> (a + b) mod 1009))
                  signals.(source a)
                  signals.(source b)
            | Cutoff n ->
                cutoff
                  (fun a b -> count (fun () -> a mod 5 = b mod 5))
                  signals.(source n)
          in
          signals.(index) <- signal;
          expected.(index) <-
            (match node with
            | Unary (n, amount) -> (expected.(source n) + amount) mod 1009
            | Binary (a, b) ->
                (expected.(source a) + expected.(source b)) mod 1009
            | Cutoff n -> expected.(source n)))
        nodes;
      let observed = Array.init size (fun _ -> ref []) in
      Array.iteri
        (fun i signal ->
          ignore
            (subscribe ~emit_initial:false signal (fun v ->
                 observed.(i) := v :: !(observed.(i)))))
        signals;
      List.iter
        (fun writes ->
          let published = Array.make size false
          and recomputed = Array.make size false in
          let previous_calls = Array.copy calls
          and previous_values = Array.copy expected in
          Array.iter (fun seen -> seen := []) observed;
          List.iter
            (fun (root, v) ->
              set roots.(root) v;
              expected.(root) <- v;
              published.(root) <- true)
            writes;
          Array.iteri
            (fun i signal ->
              int_equal "values stay stable before flush" previous_values.(i)
                (get signal))
            signals;
          Array.iteri
            (fun offset node ->
              let i = offset + 4 in
              let source n = n mod i in
              let changed, next =
                match node with
                | Unary (n, amount) ->
                    ( published.(source n),
                      (expected.(source n) + amount) mod 1009 )
                | Binary (a, b) ->
                    ( published.(source a) || published.(source b),
                      (expected.(source a) + expected.(source b)) mod 1009 )
                | Cutoff n -> (published.(source n), expected.(source n))
              in
              recomputed.(i) <- changed;
              published.(i) <-
                (changed
                &&
                match node with
                | Cutoff _ -> expected.(i) mod 5 <> next mod 5
                | _ -> true);
              if published.(i) then expected.(i) <- next)
            nodes;
          stabilize owner;
          Array.iteri
            (fun i signal ->
              int_equal "reference graph value" expected.(i) (get signal);
              require
                (!(observed.(i))
                = if published.(i) then [ expected.(i) ] else [])
                "a node published an intermediate or extra value";
              int_equal "at most one recomputation per affected node"
                (if recomputed.(i) then 1 else 0)
                (calls.(i) - previous_calls.(i)))
            signals;
          let work =
            Array.fold_left (fun n b -> n + if b then 1 else 0) 0 recomputed
            + Array.fold_left
                (fun n b -> n + if b then 1 else 0)
                0 (Array.sub published 0 4)
          in
          int_equal "scheduler work matches affected nodes" work
            (last_stabilization owner).stabilization_dirty_tasks;
          require
            ((last_stabilization owner).stabilization_rounds <= size)
            "too many stabilization rounds")
        batches)

let exception_recovery =
  property "FIFO queues recover after failures at arbitrary positions"
    ~print:(Q.Print.pair Q.Print.int Q.Print.int)
    (G.pair (G.int_range 1 50) (G.int_range 0 100))
    (fun (size, position) ->
      List.iter
        (fun enqueue ->
          let owner = scheduler () in
          let position = position mod size in
          let trace = ref [] in
          for i = 0 to size - 1 do
            enqueue owner (fun () ->
                trace := i :: !trace;
                if i = position then begin
                  enqueue owner (fun () -> trace := size :: !trace);
                  failwith "expected failure"
                end)
          done;
          (match stabilize owner with
          | () -> Q.Test.fail_report "callback failure did not propagate"
          | exception Failure _ -> ());
          stabilize owner;
          require
            (List.rev !trace = List.init (size + 1) Fun.id)
            "unexecuted tasks were lost, reordered, or run twice")
        [ enqueue_effect; enqueue_dirty ])

let subscriptions =
  property "subscriptions cancel independently, including during publication"
    ~print:(Q.Print.list Q.Print.bool) (bounded_list 60 G.bool)
    (fun cancellations ->
      let owner = scheduler () in
      let input = state owner 0 in
      let count = List.length cancellations in
      let handles = ref [] in
      let seen = Array.make count 0 in
      ignore
        (subscribe ~emit_initial:false (value input) (fun _ ->
             List.iter2
               (fun cancel handle -> if cancel then dispose_subscription handle)
               cancellations !handles));
      handles :=
        List.mapi
          (fun i _ ->
            subscribe ~emit_initial:false (value input) (fun _ ->
                seen.(i) <- seen.(i) + 1))
          cancellations;
      set input 1;
      stabilize owner;
      List.iteri
        (fun i cancelled ->
          int_equal "callback lifetime" (if cancelled then 0 else 1) seen.(i))
        cancellations;
      List.iter dispose_subscription !handles;
      set input 2;
      stabilize owner;
      List.iteri
        (fun i cancelled ->
          int_equal "cancelled callbacks remain detached"
            (if cancelled then 0 else 1)
            seen.(i))
        cancellations)

let scope_failures =
  property "scope cleanup failures still release all owned resources"
    ~print:(Q.Print.pair Q.Print.int Q.Print.int)
    (G.pair (G.int_range 1 30) (G.int_range 0 100))
    (fun (size, position) ->
      let owner = scheduler () in
      let parent = scope "parent" in
      let input = state owner 0 in
      let calls = ref 0 and cleanups = ref [] and unmounts = ref 0 in
      let children =
        List.init size (fun i ->
            let child = child_scope (string_of_int i) parent in
            ignore
              (own child
                 (subscribe ~emit_initial:false (value input) (fun _ ->
                      incr calls)));
            on_unmount child (fun () -> incr unmounts);
            on_dispose parent (fun () ->
                cleanups := i :: !cleanups;
                if i = position mod size then failwith "cleanup");
            mount child;
            child)
      in
      (match dispose_scope parent with
      | () -> Q.Test.fail_report "cleanup exception did not propagate"
      | exception Failure _ -> ());
      require
        (List.rev !cleanups = List.init size Fun.id)
        "some cleanup callbacks were skipped";
      require
        (List.for_all (fun child -> not (active child)) children)
        "a child survived parent disposal";
      int_equal "all children unmounted once" size !unmounts;
      set input 1;
      stabilize owner;
      int_equal "all owned observations were detached" 0 !calls;
      dispose_scope parent;
      int_equal "disposal is idempotent after failure" size !unmounts)

let unique_items items =
  let seen = Hashtbl.create 16 in
  List.filter
    (fun (key, _) ->
      if Hashtbl.mem seen key then false
      else begin
        Hashtbl.add seen key ();
        true
      end)
    items

let frame_gen =
  G.map unique_items (bounded_list 20 (G.pair (G.int_range 0 15) integers))

let frames_gen = G.list_size (G.int_range 1 30) frame_gen

let array_insert array index value =
  Array.init
    (Array.length array + 1)
    (fun i ->
      if i < index then array.(i)
      else if i = index then value
      else array.(i - 1))

let array_remove array index =
  Array.init
    (Array.length array - 1)
    (fun i -> array.(if i < index then i else i + 1))

let keyed_model =
  property "keyed edits preserve identity, payloads, patches, and lifetimes"
    ~print:(Q.Print.list (Q.Print.list (Q.Print.pair Q.Print.int Q.Print.int)))
    frames_gen
    (fun frames ->
      let owner = scheduler () in
      let parent = scope "collection" in
      let input = state owner [] in
      let rendered = ref [||]
      and previous = ref []
      and mounted = ref 0
      and unmounted = ref 0 in
      let signals = Hashtbl.create 16 in
      let patches = ref 0 in
      let on_patch patch =
        incr patches;
        let valid i = i >= 0 && i < Array.length !rendered in
        match patch with
        | Insert (key, i) ->
            require
              (i >= 0 && i <= Array.length !rendered)
              "invalid insertion index";
            require (not (Array.mem key !rendered)) "inserted an existing key";
            rendered := array_insert !rendered i key
        | Remove (key, i) ->
            require
              (valid i && !rendered.(i) = key)
              "removal index/key mismatch";
            rendered := array_remove !rendered i
        | Move (key, from_index, to_index) ->
            require
              (valid from_index && valid to_index
              && !rendered.(from_index) = key)
              "movement index/key mismatch";
            rendered :=
              array_insert (array_remove !rendered from_index) to_index key
      in
      let collection =
        keyed parent (value input) fst Int.compare
          (fun signal ->
            incr mounted;
            let key = fst (get signal) in
            Hashtbl.replace signals key signal;
            let sc = scope (string_of_int key) in
            on_unmount sc (fun () -> incr unmounted);
            sc)
          on_patch
      in
      List.iter
        (fun frame ->
          let old_mounts = !mounted and old_unmounts = !unmounted in
          patches := 0;
          set input
            (List.map (fun (key, payload) -> (key, fun () -> payload)) frame);
          stabilize owner;
          require
            (Array.to_list !rendered = List.map fst frame)
            "patch replay disagrees with the new order";
          let added =
            List.filter
              (fun (key, _) -> not (List.mem_assoc key !previous))
              frame
          in
          let removed =
            List.filter
              (fun (key, _) -> not (List.mem_assoc key frame))
              !previous
          in
          int_equal "mount only newly added keys" (List.length added)
            (!mounted - old_mounts);
          int_equal "unmount only removed keys" (List.length removed)
            (!unmounted - old_unmounts);
          List.iter
            (fun (key, sc) ->
              if List.mem_assoc key frame then
                require
                  (keyed_find_scope collection key == sc)
                  "retained key lost its scope"
              else require (not (active sc)) "removed key is still mounted")
            !previous;
          List.iter
            (fun (key, payload) ->
              int_equal "item payload" payload
                (snd (get (Hashtbl.find signals key)) ()))
            frame;
          require
            (!patches <= List.length !previous + List.length frame)
            "patch work exceeded removed/moved/inserted keys";
          previous :=
            List.map
              (fun (key, _) -> (key, keyed_find_scope collection key))
              frame)
        frames;
      dispose_scope parent;
      int_equal "every mounted scope was released" !mounted !unmounted;
      List.iter
        (fun (_, sc) ->
          require (not (active sc)) "item survived parent disposal")
        !previous)

let duplicate_keys =
  property "duplicate keys reject the entire edit before side effects"
    ~print:(Q.Print.pair Q.Print.int Q.Print.int) (G.pair integers integers)
    (fun (key, payload) ->
      let owner = scheduler () and parent = scope "duplicates" in
      let input = state owner [ (key, payload) ] in
      let patches = ref 0 and mounts = ref 0 in
      let collection =
        keyed parent (value input) fst Int.compare
          (fun _ ->
            incr mounts;
            scope "item")
          (fun _ -> incr patches)
      in
      let original = keyed_find_scope collection key in
      patches := 0;
      set input [ (key, payload); (key, payload + 1) ];
      (match stabilize owner with
      | () -> Q.Test.fail_report "duplicates accepted"
      | exception Invalid_argument _ -> ());
      int_equal "duplicate edit emits no patches" 0 !patches;
      int_equal "duplicate edit creates no scopes" 1 !mounts;
      require
        (keyed_find_scope collection key == original && active original)
        "duplicate edit changed scope identity";
      set input [];
      stabilize owner;
      require
        (not (active original))
        "collection cannot recover after duplicate rejection")

let clamped_moves =
  property "clamped list helpers agree with an array model and preserve entries"
    ~print:(Q.Print.triple (Q.Print.list Q.Print.int) Q.Print.int Q.Print.int)
    (G.triple (bounded_list 40 integers) integers integers)
    (fun (items, from_index, to_index) ->
      let owner = scheduler () in
      let entries =
        List.map
          (fun n -> make_keyed_entry n (state owner n) (scope "entry"))
          items
      in
      let size = List.length items in
      let expected =
        if size = 0 then []
        else begin
          let from_index = max 0 (min from_index (size - 1)) in
          let to_index = max 0 (min to_index (size - 1)) in
          let original = Array.of_list items in
          Array.to_list
            (array_insert
               (array_remove original from_index)
               to_index original.(from_index))
        end
      in
      let actual = move_entry entries from_index to_index in
      require
        (List.map keyed_entry_key actual = expected)
        "clamped movement differs from the reference";
      let inserted = make_keyed_entry 999 (state owner 999) (scope "inserted") in
      let expected =
        array_insert (Array.of_list items) (max 0 (min to_index size)) 999
      in
      require
        (List.map keyed_entry_key (insert_entry_at entries to_index inserted)
        = Array.to_list expected)
        "clamped insertion differs from the reference")

let switch_lifetimes =
  property
    "switch equality preserves branches and parent disposal releases every \
     branch"
    ~print:(Q.Print.list Q.Print.int)
    (bounded_list 80 (G.int_range 0 100))
    (fun keys ->
      let owner = scheduler () and parent = scope "switch" in
      let input = state owner 0 in
      let mounts = ref 0 and unmounts = ref 0 and branches = ref [] in
      let equal a b = a mod 5 = b mod 5 in
      let sw =
        switch parent (value input) equal (fun key ->
            incr mounts;
            let branch = scope (string_of_int key) in
            branches := branch :: !branches;
            on_unmount branch (fun () -> incr unmounts);
            branch)
      in
      let current = ref 0 and expected_mounts = ref 1 in
      List.iter
        (fun key ->
          let previous = switch_scope sw in
          set input key;
          stabilize owner;
          if equal !current key then
            require
              (switch_scope sw == previous)
              "equivalent key remounted its branch"
          else begin
            current := key;
            incr expected_mounts;
            require (not (active previous)) "replaced branch survived"
          end;
          require (active (switch_scope sw)) "current branch is inactive";
          int_equal "one mount per distinct key class" !expected_mounts !mounts;
          int_equal "one unmount per replaced branch" (!expected_mounts - 1)
            !unmounts)
        keys;
      dispose_scope parent;
      dispose_switch sw;
      int_equal "every branch released once" !mounts !unmounts;
      require
        (List.for_all (fun branch -> not (active branch)) !branches)
        "branch survived parent disposal")

let slot_lifetimes =
  property
    "state slots retain identity, isolate scopes, and release disposed scopes"
    ~print:(Q.Print.list Q.Print.int) (bounded_list 40 integers) (fun values ->
      let owner = scheduler () and parent = scope "slots" in
      let slot = state_slot "value" in
      let children =
        List.map
          (fun initial ->
            let child = child_scope "child" parent in
            let input = state_at owner child slot initial in
            require
              (input == state_at owner child slot (initial + 1000))
              "state slot changed identity";
            set input (initial + 1);
            (child, input, initial + 1))
          values
      in
      stabilize owner;
      List.iter
        (fun (_, input, expected) ->
          int_equal "state slot isolation" expected (get_state input))
        children;
      dispose_scope parent;
      int_equal "slot entries released" 0 (state_slot_count slot);
      List.iter
        (fun (child, input, _) ->
          (match state_at owner child slot 0 with
          | _ -> Q.Test.fail_report "disposed scope accepted a state slot"
          | exception Invalid_argument _ -> ());
          match subscribe (value input) ignore with
          | _ ->
              Q.Test.fail_report
                "scope did not dispose its backing state signal"
          | exception Invalid_argument _ -> ())
        children)

let initial_callback_failure =
  property "a failed initial callback rolls back only its own subscription"
    ~print:(Q.Print.list Q.Print.int)
    (G.list_size (G.int_range 1 30) integers)
    (fun values ->
      let owner = scheduler () in
      let input = state owner 0 in
      let failed_calls = ref 0 and other_calls = ref 0 in
      (match
         subscribe (value input) (fun _ ->
             incr failed_calls;
             ignore
               (subscribe ~emit_initial:false (value input) (fun _ ->
                    incr other_calls));
             failwith "initial callback")
       with
      | _ -> Q.Test.fail_report "initial callback exception did not propagate"
      | exception Failure _ -> ());
      List.iter
        (fun value ->
          set input value;
          try stabilize owner with Failure _ -> ())
        values;
      int_equal "failed subscription did not leak" 1 !failed_calls;
      int_equal "another subscription created by the callback survived"
        (List.length values) !other_calls)

let cleanup_cancellation =
  property "cleanup cancellation is respected during scope disposal"
    ~print:(Q.Print.list Q.Print.bool) (bounded_list 40 G.bool)
    (fun cancellations ->
      let sc = scope "cleanup cancellation" in
      let handles = ref [] in
      let calls = Array.make (List.length cancellations) 0 in
      on_dispose sc (fun () ->
          List.iter2
            (fun cancel handle -> if cancel then dispose_subscription handle)
            cancellations !handles);
      handles :=
        List.mapi
          (fun i _ ->
            register_cleanup sc (fun () -> calls.(i) <- calls.(i) + 1))
          cancellations;
      dispose_scope sc;
      dispose_scope sc;
      List.iteri
        (fun i cancelled ->
          int_equal "cleanup lifetime" (if cancelled then 0 else 1) calls.(i))
        cancellations)

let allocated_words f =
  let before = Gc.allocated_bytes () in
  f ();
  (Gc.allocated_bytes () -. before) /. float_of_int (Sys.word_size / 8)

let allocation_budget label size multiplier words =
  let budget = float_of_int ((multiplier * size) + 2048) in
  if words > budget then
    Q.Test.fail_reportf "%s: n=%d allocated %.0f words, budget %.0f" label size
      words budget

let stress_size = G.int_range 128 512

let queue_allocations =
  property ~count:30 "FIFO queue allocation is linear in queued work"
    ~print:Q.Print.int stress_size (fun size ->
      List.iter
        (fun enqueue ->
          let owner = scheduler () in
          let calls = ref 0 in
          let task () = incr calls in
          let words =
            allocated_words (fun () ->
                for _ = 1 to size do
                  enqueue owner task
                done;
                stabilize owner)
          in
          int_equal "all FIFO work executes" size !calls;
          allocation_budget "FIFO" size 64 words)
        [ enqueue_dirty; enqueue_effect ])

let fanout_allocations =
  property ~count:30 "wide graphs batch once per node with linear allocation"
    ~print:Q.Print.int stress_size (fun size ->
      let owner = scheduler () in
      let input = state owner 0 in
      let calls = ref 0 in
      let signals =
        List.init size (fun _ ->
            map
              (fun n ->
                incr calls;
                n + 1)
              (value input))
      in
      calls := 0;
      for i = 1 to 20 do
        set input i
      done;
      let words = allocated_words (fun () -> stabilize owner) in
      int_equal "one transform per affected node" size !calls;
      int_equal "one publication plus one task per node" (size + 1)
        (last_stabilization owner).stabilization_dirty_tasks;
      require
        (List.for_all (fun signal -> get signal = 21) signals)
        "wide graph did not settle";
      allocation_budget "fanout" size 128 words)

let subscription_allocations =
  property ~count:30
    "subscription registration and teardown allocation is linear"
    ~print:Q.Print.int stress_size (fun size ->
      let input = constant (scheduler ()) 0 in
      let handles = ref [] in
      let words =
        allocated_words (fun () ->
            for _ = 1 to size do
              handles := subscribe ~emit_initial:false input ignore :: !handles
            done;
            List.iter dispose_subscription !handles)
      in
      allocation_budget "subscriptions" size 96 words)

let scope_allocations =
  property ~count:30 "scope creation and teardown allocation is linear"
    ~print:Q.Print.int stress_size (fun size ->
      let parent = scope "parent" in
      let input = constant (scheduler ()) 0 in
      let unmounts = ref 0 in
      let words =
        allocated_words (fun () ->
            for _ = 1 to size do
              let child = child_scope "child" parent in
              ignore (own child (subscribe ~emit_initial:false input ignore));
              on_unmount child (fun () -> incr unmounts);
              mount child
            done;
            dispose_scope parent)
      in
      int_equal "all scopes unmount" size !unmounts;
      (* Per-scope counter accounting adds bounded storage, independent of n;
         measured at ~201 words per iteration. *)
      allocation_budget "scopes" size 224 words)

let logarithmic_depth size =
  let rec loop n depth =
    if n <= 1 then depth else loop ((n + 1) / 2) (depth + 1)
  in
  loop size 1

let keyed_comparisons =
  property ~count:20 "keyed reversal comparison work grows at most n log n"
    ~print:Q.Print.int (G.int_range 512 1024) (fun size ->
      let owner = scheduler () and parent = scope "keyed" in
      let initial = List.init size Fun.id in
      let input = state owner initial in
      let comparisons = ref 0 in
      let compare a b =
        incr comparisons;
        Int.compare a b
      in
      let moves = ref 0 in
      ignore
        (keyed parent (value input) Fun.id compare
           (fun _ -> scope "item")
           (function Move _ -> incr moves | _ -> ()));
      comparisons := 0;
      set input (List.rev initial);
      stabilize owner;
      int_equal "reversal needs exactly n-1 moves" (size - 1) !moves;
      let budget = 16 * size * logarithmic_depth size in
      if !comparisons > budget then
        Q.Test.fail_reportf "n=%d used %d comparisons, n log n budget=%d" size
          !comparisons budget;
      dispose_scope parent)

let publication_failures =
  property "publication failures notify all live dependents and recover"
    ~print:(Q.Print.pair Q.Print.int Q.Print.int)
    (G.pair (G.int_range 1 40) (G.int_range 0 100))
    (fun (size, position) ->
      let owner = scheduler () in
      let input = state owner 0 in
      let seen = Array.make size 0 in
      let failed = position mod size in
      let handles = Array.init size (fun i ->
        subscribe ~emit_initial:false (value input) (fun v ->
          seen.(i) <- v;
          if i = failed then failwith "publication")) in
      let derived = map (( * ) 2) (value input) in
      set input 1;
      (match stabilize owner with
      | () -> Q.Test.fail_report "publication failure did not propagate"
      | exception Failure _ -> ());
      dispose_subscription handles.(failed);
      stabilize owner;
      require (Array.for_all ((=) 1) seen) "a live callback missed publication";
      int_equal "downstream survived observer exception" 2 (get derived);
      set input 2;
      stabilize owner;
      int_equal "later downstream update" 4 (get derived))

let registry_cancellation =
  property "arbitrary cleanup cancellations preserve live counts and bounded storage"
    ~print:(Q.Print.list Q.Print.int)
    (bounded_list 100 (G.int_range 0 63)) (fun cancellations ->
      let sc = scope "registry" in
      let calls = Array.make 64 0 and cancelled = Array.make 64 false in
      let handles = Array.init 64 (fun i ->
        register_cleanup sc (fun () -> calls.(i) <- calls.(i) + 1)) in
      List.iter (fun i ->
        cancelled.(i) <- true;
        dispose_subscription handles.(i);
        let live = Array.fold_left (fun count dead -> if dead then count else count+1) 0 cancelled in
        int_equal "live registrations" live (scope_cleanup_count sc);
        require (List.length (scope_cleanup_entries sc) <= 2 * live) "unbounded tombstones") cancellations;
      dispose_scope sc;
      Array.iteri (fun i count ->
        int_equal "uncancelled cleanup runs exactly once" (if cancelled.(i) then 0 else 1) count) calls;
      int_equal "disposed registry is empty" 0 (scope_cleanup_count sc))

let () =
  QCheck_base_runner.run_tests_main
    [
      batching;
      graphs;
      exception_recovery;
      subscriptions;
      scope_failures;
      keyed_model;
      duplicate_keys;
      clamped_moves;
      switch_lifetimes;
      slot_lifetimes;
      initial_callback_failure;
      cleanup_cancellation;
      queue_allocations;
      fanout_allocations;
      subscription_allocations;
      scope_allocations;
      keyed_comparisons;
      publication_failures;
      registry_cancellation;
    ]
