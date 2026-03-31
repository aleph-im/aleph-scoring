{
  config,
  lib,
  pkgs,
  modulesPath,
  ...
}: {
  imports = [
    # Built-in NixOS Digital Ocean support:
    # - GRUB bootloader, virtio drivers
    # - Fetches IPs, gateways, DNS from metadata service
    # - Imports SSH keys from DO account
    (modulesPath + "/virtualisation/digital-ocean-config.nix")
  ];

  # Disk layout for nixos-anywhere via disko
  disko.memSize = 4096; # MB - default 1024 is too low for image builds

  disko.devices.disk.main = {
    device = "/dev/vda";
    type = "disk";
    imageSize = "8G";
    content = {
      type = "gpt";
      partitions = {
        root = {
          size = "100%";
          content = {
            type = "filesystem";
            format = "ext4";
            mountpoint = "/";
          };
        };
      };
    };
  };

  # Don't rebuild NixOS config from DO user-data
  virtualisation.digitalOcean.rebuildFromUserData = false;

  networking.hostName = "aleph-scoring";
  networking.firewall.allowedTCPPorts = [22];

  # Prevent 'too many open files'
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

  # Root SSH access for deployment (DO metadata also imports keys)
  users.users.root.openssh.authorizedKeys.keys = [
    "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCv/02pOyoKB0X4SJgM9Zkv75rvOTfN2HpRiyZKY2Buk+Q4ry17oX2vgFycZ3vMIaTvHkzq5HP9kXRzRGdVC0htmuZkrFH1y3D9JfsdQE2HOtZcgvAHistWFV82WfrY9ASx90OhPeJGWnQFmAkbLAl0WYmr11WbJqSLWa/xh63ioEH/qtyCspXJCAUwk3KlMPmi/owA2gCwnFyUcde4bBM2Af9IwCmg35O7yxIZN/2Vd8XwE/uKpeWKHA/OEGOlICzCNKfOImmcI0xt7paGuFWuDM1PyGOfemDv1mFAobEv5xpFAk/DhLTXrvjAvTZYVSzoFwMm2vlzWCl7h5EY3SGr alie"
  ];

  nix.settings.experimental-features = ["nix-command" "flakes"];

  environment.systemPackages = with pkgs; [
    vim
    git
    htop
  ];

  environment.shellAliases = {
    journal = "journalctl -u aleph-scoring-measure --boot";
  };

  system.stateVersion = "25.11";
}
