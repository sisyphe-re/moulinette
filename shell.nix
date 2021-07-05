#!/usr/bin/env nix-shell
with import <nixpkgs> { overlays = [ ]; };

let
  my-packages = [
    sqlite
    sqlite.dev
    sqlitecpp
    cargo
    rustc
    zstd
    linuxPackages.perf
  ];
in
mkShell {
  RUSTFLAGS="-C target-cpu=native";
  buildInputs = [
    my-packages
  ];
}
