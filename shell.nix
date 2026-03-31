{
  pkgs ? import <nixpkgs> {},
}: let
  alephScoring = pkgs.callPackage ./default.nix {};
in
  pkgs.mkShell {
    name = "metrics-env";

    buildInputs = [
      pkgs.python3
      alephScoring
    ];

    shellHook = ''
      echo "Run 'scoring measure' to measure the node metrics"
    '';

    ALEPH_SCORING_ASN_DB_DIRECTORY = "./asn";
  }
