let
  pkgs = import (builtins.fetchGit {
    url = "https://github.com/NixOS/nixpkgs.git";
    ref = "master";
    rev = "e9f8cddbcd2167cf8eecb7ee6637d1e078018b4f";
  }) {
    overlays = [
      (import ./nix/mayastor-overlay.nix)
    ];
  };
in
nixpkgs
