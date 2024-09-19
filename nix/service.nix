{ pkgs, ... }:
let
  alephScoring = pkgs.callPackage
    (pkgs.fetchFromGitHub {
      owner = "aleph-im";
      repo = "aleph-scoring";
      rev = "b026ce63b76265ad749945a8c184ec080512c0bb";
      sha256 = "sha256-ClwjGVnS3SEWZ3BL6M5jRwW695imEOV25PNbQOIeR8U=";
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
      # Every hours
      OnCalendar = "*-*-* *:00:00";
      Persistent = true;
    };
  };

  environment.systemPackages = with pkgs; [
    alephScoring
    btop
  ];

  # Systems with low RAM (1GB) may have issues when rebuilding
  swapDevices = [ {
    device = "/var/lib/swapfile";
    size = 4*1024; # in megabytes
  } ];

  services.prometheus.exporters.node = {
    enable = true;
    enabledCollectors = [ "systemd" ];
    openFirewall = true;
  };
}
