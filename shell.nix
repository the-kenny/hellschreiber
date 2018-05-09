{ nixpkgs ? <nixpkgs> }:
let
  pkgs = import nixpkgs {};
in rec {
  rustEnv = pkgs.stdenv.mkDerivation {
    name = "rust";
    version = "1.2.3.4";
    src = null;
    buildInputs = with pkgs; [
      sqlite
      pkgconfig
    ];

    # RUST_BACKTRACE="1";
    RUSTUP_TOOLCHAIN = "stable";
  };
}
