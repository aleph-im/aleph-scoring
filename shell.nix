{ pkgs ? import <nixpkgs> { } }:
let
  # Fetch the package locally
  alephScoringLocal = pkgs.callPackage ./default.nix { };

  # Alternatively, you can fetch the package from GitHub
  alephScoring = pkgs.callPackage
    (pkgs.fetchFromGitHub {
      owner = "aleph-im";
      repo = "aleph-scoring";
      rev = "b0b652465b65db0947fdbeac72ee8cf86f717937";
      sha256 = "sha256-+rYKzzxQTrJ3AO2nKK6Y4+gHKvLNB8LzBABa2h4F+UA=";
    })
    { };
in
pkgs.mkShell {
  name = "metrics-env";

  buildInputs = [
    pkgs.python3
    alephScoring
    # alephScoringLocal
  ];

  shellHook = ''
    echo "Run 'scoring measure' to measure the node metrics"
  '';

  ALEPH_SCORING_ASN_DB_DIRECTORY = "./asn";
}
