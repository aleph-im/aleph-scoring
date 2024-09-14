{ pkgs, ... }:
let
  alephScoring = pkgs.callPackage
    (pkgs.fetchFromGitHub {
      owner = "aleph-im";
      repo = "aleph-scoring";
      rev = "hoh-add-nix";
      sha256 = "sha256-uXoMzCjGWZbyaca1kF4i628MTLeSjIdIRoWqyVN5y78=";
    })
    { };
in
{
  systemd.tmpfiles.rules = [
    "d /var/lib/asn 0755 root root -"
  ];

  systemd.services.aleph-scoring-measure = {
    enable = true;
    description = "Regular run of the metrics";
    serviceConfig = {
      Type = "oneshot";
      # ExecStartPre = "mkdir -p /srv/asn";
      ExecStart = "${alephScoring}/bin/scoring measure --publish";
    };
    environment = {
      ALEPH_SCORING_ASN_DB_DIRECTORY = "/var/lib/asn";
      ALEPH_SCORING_ETHEREUM_PRIVATE_KEY_PATH = "/srv/secrets/aleph.im/ethereum.key";
    };
  };

  systemd.timers.aleph-scoring-measure = {
    enable = true;
    description = "Timer for the NixOS rebuild service";
    wantedBy = [ "timers.target" ];
    timerConfig = {
      # Every hours + 5 minutes
      OnCalendar = "*-*-* *:05:00";
      Persistent = true;
    };
  };

  environment.systemPackages = with pkgs; [
    alephScoring
  ];

  services.prometheus.exporters.node = {
    enable = true;
    enabledCollectors = [ "systemd" ];
    openFirewall = true;
  };
}
