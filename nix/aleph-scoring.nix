{ pkgs }:
{
  alephScoring = pkgs.callPackage (pkgs.fetchFromGitHub {
    owner = "aleph-im";
    repo = "aleph-scoring";
    rev = "b07ac8bc2bfaddd600f949fd5125c64d8e457318";
    sha256 = "sha256-oSfGZhwAwPC6yzLGysgjyTNaCMH5AC0lcObrPTTcqYw=";
  }) { };
}
