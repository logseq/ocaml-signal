open Signal
let measured f = let before = Gc.allocated_bytes () in f (); Gc.allocated_bytes () -. before
let teardown size keyed_children =
  let parent = scope "parent" and owner = scheduler () in
  if keyed_children then (
    let input = state owner (List.init size Fun.id) in
    ignore (keyed parent (value input) Fun.id Int.compare (fun _ -> child_scope "child" parent) ignore);
    measured (fun () -> set input []; stabilize owner))
  else (
    let children = List.init size (fun _ -> child_scope "child" parent) in
    measured (fun () -> List.iter dispose_scope children))
let () =
  List.iter (fun keyed_children ->
    let small = teardown 256 keyed_children and large = teardown 1024 keyed_children in
    Printf.printf "keyed=%b bytes256=%.0f bytes1024=%.0f ratio=%.2f\n%!" keyed_children small large (large /. small);
    if large > 6. *. small then failwith "child teardown allocation grows faster than linear budget") [false;true];
  let owned size =
    let owner = scheduler () and parent = scope "parent" in
    let source = constant owner 0 in
    let switches = List.init size (fun _ -> switch parent source (=) (fun _ -> scope "branch")) in
    measured (fun () -> List.iter dispose_switch switches)
  in
  let small = owned 256 and large = owned 1024 in
  Printf.printf "owned bytes256=%.0f bytes1024=%.0f ratio=%.2f\n%!" small large (large /. small);
  if large > 6. *. small then failwith "owned lifetime teardown grows faster than linear budget"
