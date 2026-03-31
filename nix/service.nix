{
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.services.aleph-scoring;
in {
  options.services.aleph-scoring = {
    enable = lib.mkEnableOption "aleph-scoring metrics service";

    package = lib.mkOption {
      type = lib.types.package;
      description = "The aleph-scoring package to use.";
    };

    ethereumKeyPath = lib.mkOption {
      type = lib.types.str;
      default = "/srv/secrets/aleph.im/ethereum.key";
      description = "Path to the Ethereum private key file.";
    };

    sentryDsnPath = lib.mkOption {
      type = lib.types.str;
      default = "/srv/secrets/aleph.im/sentry-dsn.txt";
      description = "Path to the Sentry DSN file.";
    };

    asnDbDirectory = lib.mkOption {
      type = lib.types.str;
      default = "/var/lib/asn";
      description = "Directory for ASN database files.";
    };

    onCalendar = lib.mkOption {
      type = lib.types.str;
      default = "*-*-* *:00:00";
      description = "Systemd calendar expression for the metrics timer.";
    };
  };

  config = lib.mkIf cfg.enable {
    systemd.tmpfiles.rules = ["d ${cfg.asnDbDirectory} 0755 root root -"];

    systemd.services.aleph-scoring-measure = {
      description = "Regular run of the metrics";
      serviceConfig = {
        Type = "oneshot";
        ExecStart = "${cfg.package}/bin/scoring measure --publish";
      };
      environment = {
        ALEPH_SCORING_ASN_DB_DIRECTORY = cfg.asnDbDirectory;
        ALEPH_SCORING_ETHEREUM_PRIVATE_KEY_PATH = cfg.ethereumKeyPath;
        ALEPH_SCORING_SENTRY_DSN_PATH = cfg.sentryDsnPath;
      };
    };

    systemd.timers.aleph-scoring-measure = {
      description = "Timer for aleph-scoring metrics collection";
      wantedBy = ["timers.target"];
      timerConfig = {
        OnCalendar = cfg.onCalendar;
        Persistent = true;
      };
    };

    environment.systemPackages = [cfg.package pkgs.btop];

    # Low-RAM droplets need swap for nixos-rebuild
    swapDevices = [
      {
        device = "/var/lib/swapfile";
        size = 2 * 1024;
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
      enabledCollectors = ["systemd"];
      openFirewall = true;
    };

    nix.settings.auto-optimise-store = true;
  };
}
