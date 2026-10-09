open Signal

let warmups = 3
let samples = 9
let sizes = [ 128; 256; 512; 1024; 2048 ]

let fifo size =
  let owner = scheduler () in
  let calls = ref 0 in
  let task () = incr calls in
  fun () ->
    for _ = 1 to size do
      enqueue_dirty owner task
    done;
    stabilize owner;
    assert (!calls = size)

let fanout size =
  let owner = scheduler () in
  let input = state owner 0 in
  let calls = ref 0 in
  for _ = 1 to size do
    ignore
      (map
         (fun n ->
           incr calls;
           n + 1)
         (value input))
  done;
  calls := 0;
  fun () ->
    set input 1;
    stabilize owner;
    assert (!calls = size)

let chain size =
  let owner = scheduler () in
  let input = state owner 0 in
  let current = ref (value input) in
  for _ = 1 to size do
    current := map (( + ) 1) !current
  done;
  fun () ->
    set input 1;
    stabilize owner;
    assert (get !current = size + 1)

let subscriptions size =
  let input = constant (scheduler ()) 0 in
  fun () ->
    let handles =
      List.init size (fun _ -> subscribe ~emit_initial:false input ignore)
    in
    List.iter dispose_subscription handles

let keyed_reverse size =
  let owner = scheduler () and parent = scope "benchmark" in
  let initial = List.init size Fun.id in
  let input = state owner initial in
  let moves = ref 0 in
  ignore
    (keyed parent (value input) Fun.id Int.compare
       (fun _ -> scope "item")
       (function Move _ -> incr moves | _ -> ()));
  fun () ->
    set input (List.rev initial);
    stabilize owner;
    assert (!moves = size - 1)

let measure prepare size =
  let run = prepare size in
  Gc.full_major ();
  let before_words = Gc.allocated_bytes () in
  let before_time = Unix.gettimeofday () in
  run ();
  let elapsed = Unix.gettimeofday () -. before_time in
  let words =
    (Gc.allocated_bytes () -. before_words) /. float_of_int (Sys.word_size / 8)
  in
  (elapsed *. 1000., words)

let median values =
  let values = Array.of_list values in
  Array.sort Float.compare values;
  values.(Array.length values / 2)

let () =
  Printf.printf
    "Native OCaml %s; warmups=%d, samples=%d; medians; construction excluded \
     except FIFO/subscription operations\n"
    Sys.ocaml_version warmups samples;
  Printf.printf "workload,n,median_ms,allocated_words\n%!";
  List.iter
    (fun (name, prepare) ->
      List.iter
        (fun size ->
          for _ = 1 to warmups do
            ignore (measure prepare size)
          done;
          let measurements =
            List.init samples (fun _ -> measure prepare size)
          in
          Printf.printf "%s,%d,%.4f,%.0f\n%!" name size
            (median (List.map fst measurements))
            (median (List.map snd measurements)))
        sizes)
    [
      ("fifo", fifo);
      ("fanout", fanout);
      ("chain", chain);
      ("subscriptions", subscriptions);
      ("keyed_reverse", keyed_reverse);
    ]
