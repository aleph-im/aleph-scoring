{
  pkgs,
  lib,
  modulesPath,
  ...
}: {
  imports = [
    "${modulesPath}/virtualisation/proxmox-lxc.nix"
    ./service.nix
  ];

  # Proxmox LXC container (unprivileged): no bootloader, no kernel modules,
  # no swapDevices. The proxmox module also configures systemd-networkd to
  # pick up the network set by Proxmox.
  system.stateVersion = "24.11";

  services.openssh = {
    enable = true;
    settings.PermitRootLogin = "prohibit-password";
  };
  users.users.root.openssh.authorizedKeys.keys = lib.mkDefault [];

  services.aleph-scoring = {
    enable = true;
    package = lib.mkDefault (pkgs.callPackage ./default.nix {});
    ethereumKeyPath = "/srv/secrets/aleph.im/ethereum.key";
    # Runtime overrides, editable inside the container without a rebuild.
    environmentFile = "/etc/aleph-scoring.env";
  };

  # Secrets are pushed into the container after creation, never built into the
  # image.
  systemd.tmpfiles.rules = [
    "d /srv/secrets/aleph.im 0700 root root -"
  ];
}
