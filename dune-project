(lang dune 3.17)
(using melange 0.1)

(name ocaml-signal)
(version 0.1.0)

(generate_opam_files true)

(package
 (name ocaml-signal)
 (maintainers "Tienson Qin <tiensonqin@gmail.com>")
 (authors "Tienson Qin")
 (license MIT)
 (synopsis "A small statically typed incremental runtime for OCaml")
 (description
  "Reactive values, scoped state, lifecycle, effects, switches, and keyed collections. OCaml port of signal-lg.")
 (depends
  (ocaml (>= 5.1))
  (dune (>= 3.17))
  (alcotest :with-test)))
