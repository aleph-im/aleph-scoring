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
        boot = {
          size = "1M";
          type = "EF02"; # BIOS boot partition for GRUB on GPT
        };
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

  # Disable DO module's GRUB — disko handles it via the boot partition
  boot.loader.grub.device = lib.mkForce "nodev";
  boot.loader.grub.devices = lib.mkForce ["/dev/vda"];
  boot.loader.grub.mirroredBoots = lib.mkForce [
    {path = "/boot"; devices = ["/dev/vda"];}
  ];

  networking.hostName = "aleph-scoring";
  networking.useDHCP = lib.mkForce false;
  networking.firewall.allowedTCPPorts = [22];

  # Configure networking from DO metadata API (DHCP is not available)
  systemd.services.do-network-setup = {
    description = "Configure networking from DigitalOcean metadata";
    wantedBy = ["multi-user.target"];
    wants = ["network.target"];
    after = ["network.target" "dhcpcd.service"];
    before = ["sshd.service"];
    serviceConfig = {
      Type = "oneshot";
      RemainAfterExit = true;
    };
    path = [pkgs.curl pkgs.iproute2 pkgs.gawk];
    script = ''
      set -euo pipefail
      META="http://169.254.169.254/metadata/v1"

      # Bring interface up so we can reach the metadata service
      ip link set ens3 up
      # Add link-local route to metadata service
      ip route add 169.254.169.254/32 dev ens3 || true
      # Wait for link to be ready
      sleep 2

      # Convert netmask to prefix length (pure bash)
      mask2prefix() {
        local mask="$1" prefix=0
        for octet in $(echo "$mask" | tr '.' ' '); do
          case $octet in
            255) prefix=$((prefix + 8)) ;;
            254) prefix=$((prefix + 7)) ;;
            252) prefix=$((prefix + 6)) ;;
            248) prefix=$((prefix + 5)) ;;
            240) prefix=$((prefix + 4)) ;;
            224) prefix=$((prefix + 3)) ;;
            192) prefix=$((prefix + 2)) ;;
            128) prefix=$((prefix + 1)) ;;
            0) ;;
          esac
        done
        echo "$prefix"
      }

      # Public interface (ens3)
      IP4=$(curl -sf "$META/interfaces/public/0/ipv4/address")
      MASK4=$(curl -sf "$META/interfaces/public/0/ipv4/netmask")
      GW4=$(curl -sf "$META/interfaces/public/0/ipv4/gateway")
      PREFIX=$(mask2prefix "$MASK4")

      ip addr add "$IP4/$PREFIX" dev ens3 || true
      ip route replace default via "$GW4" dev ens3

      # Anchor IP (private network on public interface)
      ANCHOR=$(curl -sf "$META/interfaces/public/0/anchor_ipv4/address" || true)
      if [ -n "$ANCHOR" ]; then
        ANCHOR_MASK=$(curl -sf "$META/interfaces/public/0/anchor_ipv4/netmask")
        ANCHOR_PREFIX=$(mask2prefix "$ANCHOR_MASK")
        ip addr add "$ANCHOR/$ANCHOR_PREFIX" dev ens3 || true
      fi

      # IPv6
      IP6=$(curl -sf "$META/interfaces/public/0/ipv6/address" || true)
      if [ -n "$IP6" ]; then
        GW6=$(curl -sf "$META/interfaces/public/0/ipv6/gateway")
        ip -6 addr add "$IP6/64" dev ens3 || true
        ip -6 route add default via "$GW6" dev ens3 || true
      fi

      # Private interface (ens4) if present
      PRIV_IP=$(curl -sf "$META/interfaces/private/0/ipv4/address" || true)
      if [ -n "$PRIV_IP" ]; then
        PRIV_MASK=$(curl -sf "$META/interfaces/private/0/ipv4/netmask")
        PRIV_PREFIX=$(mask2prefix "$PRIV_MASK")
        ip addr add "$PRIV_IP/$PRIV_PREFIX" dev ens4 || true
        ip link set ens4 up || true
      fi

      # DNS
      DNS=$(curl -sf "$META/dns/nameservers" | head -2)
      : > /etc/resolv.conf
      for ns in $DNS; do
        echo "nameserver $ns" >> /etc/resolv.conf
      done
    '';
  };

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

  # Temporary root password for recovery console (password: nixos)
  users.users.root.hashedPassword = lib.mkForce "$y$j9T$1sjiP/C5irLoVVjexJwVT0$nGFYJDtuBXjAi2ceGzciO5cO5cx6B.f9nweMtCoM9O/";

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
