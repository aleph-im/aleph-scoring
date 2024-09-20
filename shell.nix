{
  pkgs ? import <nixpkgs> { },
}:
let
  # Fetch the package locally
  alephScoringLocal = pkgs.callPackage ./default.nix { };

  # Alternatively, you can fetch the package from GitHub
  inherit (import ./nix/aleph-scoring.nix { inherit pkgs; }) alephScoring;
in
pkgs.mkShell {
  name = "metrics-env";

  buildInputs = [
    pkgs.python3
    alephScoring
    # Uncomment the following line if you prefer the local version
    # (Uncomment only one of alephScoring or alephScoringLocal)
    # alephScoringLocal
  ];

  shellHook = ''
    echo "Run 'scoring measure' to measure the node metrics"
  '';

  ALEPH_SCORING_ASN_DB_DIRECTORY = "./asn";
}
