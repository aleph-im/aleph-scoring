{ pkgs ? import <nixpkgs> { } }:
let
  # Fetch the package locally
  alephScoringLocal = pkgs.callPackage ./default.nix { };

  # Alternatively, you can fetch the package from GitHub
  alephScoring = pkgs.callPackage
    (pkgs.fetchFromGitHub {
      owner = "aleph-im";
      repo = "aleph-scoring";
      rev = "hoh-add-nix";
      sha256 = "sha256-SkLOWKql6RtdGQPIUDkqFHgCa98OzKWFL8/bQyXXHdM=";
    })
    { };
in
pkgs.mkShell {
  name = "metrics-env";

  buildInputs = [
    pkgs.python3
    # alephScoring
    alephScoringLocal
  ];

  shellHook = ''
    echo "Run 'scoring measure' to measure the node metrics"
  '';

  ALEPH_SCORING_ASN_DB_DIRECTORY = "./asn";
}
