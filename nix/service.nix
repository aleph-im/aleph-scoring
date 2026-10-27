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

    environmentFile = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = "/etc/aleph-scoring.env";
      description = ''
        Optional systemd EnvironmentFile for ALEPH_SCORING_* overrides.
        The file is created empty if missing and can be edited at runtime
        without rebuilding the system. Values here override `extraEnvironment`.
      '';
    };

    extraEnvironment = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = {};
      description = "Additional ALEPH_SCORING_* environment variables.";
    };
  };

  config = lib.mkIf cfg.enable {
    systemd.tmpfiles.rules =
      ["d ${cfg.asnDbDirectory} 0755 root root -"]
      ++ lib.optional (cfg.environmentFile != null) (
        "f ${cfg.environmentFile} 0600 root root -"
      );

    systemd.services.aleph-scoring-measure = {
      description = "Regular run of the metrics";
      serviceConfig = {
        Type = "oneshot";
        ExecStart = "${cfg.package}/bin/scoring measure --publish";
      } // (
        if cfg.environmentFile == null
        then {}
        else {EnvironmentFile = cfg.environmentFile;}
      );
      environment =
        {
          ALEPH_SCORING_ASN_DB_DIRECTORY = cfg.asnDbDirectory;
          ALEPH_SCORING_ETHEREUM_PRIVATE_KEY_PATH = cfg.ethereumKeyPath;
          ALEPH_SCORING_SENTRY_DSN_PATH = cfg.sentryDsnPath;
        }
        // cfg.extraEnvironment;
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
