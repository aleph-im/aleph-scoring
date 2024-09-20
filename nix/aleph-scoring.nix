{ pkgs }:
{
  alephScoring = pkgs.callPackage (pkgs.fetchFromGitHub {
    owner = "aleph-im";
    repo = "aleph-scoring";
    rev = "b0b652465b65db0947fdbeac72ee8cf86f717937";
    sha256 = "sha256-+rYKzzxQTrJ3AO2nKK6Y4+gHKvLNB8LzBABa2h4F+UA=";
  }) { };
}
