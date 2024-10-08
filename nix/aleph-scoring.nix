{ pkgs }:
{
  alephScoring = pkgs.callPackage (pkgs.fetchFromGitHub {
    owner = "aleph-im";
    repo = "aleph-scoring";
    rev = "5ae3ad6b4682087cff1c0c1558811359fcb77ab3";
    sha256 = "sha256-0vAI+DOmF6/Z24iwjzt00EEsoosOis43PSbX5TWE6oo=";
  }) { };
}
