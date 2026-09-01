# Bundled Joker worker

Legion executes Joker as a separately identifiable supervised worker process.
Release builds pin `github.com/rcarmo/go-joker` **v1.8.0**, commit
`edd0fe7fff7b2bae3a714a9918502f7dd3b21d5f`, module checksum
`h1:GU+0R2sVzhFROnlpGyzy/jX6/qYf5+gx7o3n29qHZpo=`.

The upstream module declares its historical module path as
`github.com/candid82/joker`; the `Makefile` therefore downloads the reviewed
fork archive and builds it from that immutable source directory rather than
adding Joker to Legion's module graph. No Joker source files are copied into or
modified by Legion. Source is available at <https://github.com/rcarmo/go-joker/tree/edd0fe7fff7b2bae3a714a9918502f7dd3b21d5f>.

Joker is licensed under EPL-1.0. The complete license is in `LICENSE`.
