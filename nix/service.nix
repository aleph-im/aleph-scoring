{ pkgs, ... }:
let
  inherit (import ./aleph-scoring.nix { inherit pkgs; }) alephScoring;
in
{
  systemd.tmpfiles.rules = [ "d /var/lib/asn 0755 root root -" ];

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
      ALEPH_SCORING_SENTRY_DSN_PATH = "/srv/secrets/aleph.im/sentry-dsn.txt";
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
  swapDevices = [
    {
      device = "/var/lib/swapfile";
      size = 4 * 1024; # in megabytes
    }
  ];

  services.openssh = {
    enable = true;
    settings = {
      PasswordAuthentication = false;
      KbdInteractiveAuthentication = false;
      PermitRootLogin = "prohibit-password";
    };
  };

  services.prometheus.exporters.node = {
    enable = true;
    enabledCollectors = [ "systemd" ];
    openFirewall = true;
  };
}
