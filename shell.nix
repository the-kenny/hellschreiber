{ pkgs ? import <nixpkgs> {} }:

let
  stdenv = pkgs.stdenv;
  rustNightly = import ../nightly.nix {};
in
stdenv.mkDerivation {
  name = "rust-env";
  buildInputs = with pkgs; [
    (rustNightly.rust-platform {})
    # rustNightly.cargo
    sqlite
  ];

  RUST_BACKTRACE=1;
  RUST_SRC_PATH="${pkgs.rustc.src}";
}
