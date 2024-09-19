{
  modulesPath,
  pkgs,
  config,
  lib,
  ...
}:
{
  imports = [
    (modulesPath + "/profiles/qemu-guest.nix")
    ./service.nix
  ];
  boot.initrd.availableKernelModules = [
    "ata_piix"
    "uhci_hcd"
    "virtio_pci"
    "sr_mod"
    "virtio_blk"
  ];
  boot.initrd.kernelModules = [ ];
  boot.kernelModules = [ "kvm-intel" ];
  boot.extraModulePackages = [ ];

  nixpkgs.localSystem.system = "x86_64-linux";
  virtualisation.cores = 4;
  virtualisation.memorySize = 8192;
  virtualisation.diskSize = 12000;
  # virtualisation.interfaces.enp5s0.assignIP = true;
  virtualisation.forwardPorts = [
    # { from = "host"; host.port = 2222; guest.port = 22; }
    # { from = "host"; host.port = 4020; guest.port = 4020; }
  ];

  # set hostname
  networking.hostName = "aleph-metrics";

  users.users = {
    hugo = {
      isNormalUser = true;
      extraGroups = [ "wheel" ];
      openssh.authorizedKeys.keys = [
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDuwLTvcZOizLhXFb5YspQ6IYNIU7xGGizpALP0Q3Fjc"
      ];
    };
  };
  services.getty.autologinUser = "root";

  services.openssh.enable = true;

  environment = {
    shellAliases = {
      journal = "journalctl -u aleph-scoring-measure --boot";
    };
  };

  # Prevent 'too many open files' error:
  security.pam.loginLimits = [
    {
      domain = "*";
      type = "soft";
      item = "nofile";
      value = "8192";
    }
    {
      domain = "*";
      type = "hard";
      item = "nofile";
      value = "32768";
    }
  ];

  environment.systemPackages = with pkgs; [
    vim
    helix
    git
  ];

  system.stateVersion = "24.11";
}
