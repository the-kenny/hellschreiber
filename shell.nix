{ nixpkgs ? <nixpkgs> }:
let
  pkgs = import nixpkgs {};
in rec {
  rustEnv = pkgs.stdenv.mkDerivation {
    name = "rust";
    version = "1.2.3.4";
    src = null;
    buildInputs = with pkgs; [
      rustChannels.stable.rust
      rustChannels.stable.cargo
      sqlite
      pkgconfig
    ];

    RUST_BACKTRACE=1;
    RUST_SRC_PATH="${pkgs.rustc.src}";

    shellHook = ''
      export PATH="target/debug/:$PATH";
    '';
  };
}
