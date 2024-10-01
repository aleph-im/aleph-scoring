
## Metrics

Metrics are meant to be run from many different places in order to get a good
picture of the network on a global scale and limit geographic and network 
configuration bias.

While the ideal configuration would be to have aleph.im Core Channel Nodes
run or supervise these measurements, a quick way to get started is to run
the metrics from a collection of VPSs around the world from different providers.

### Setting up on VPS with NixOS

An easy way to ease the maintenance of the VPSs is to use NixOS, which is a
declarative Linux distribution that allows you to define the entire configuration
of the system in a single file.

The procedure we used relied on the `nixos-infect` script, which is a simple
script that converts an existing operating system to NixOS. The script has
been tested on multiple hosting providers.

https://github.com/elitak/nixos-infect

Make sure to add your SSH key to the root user of the VPS before running the
script.

Once the system has been converted to NixOS and has been rebooted, you can
proceed to add the configuration specific for the metrics.

#### OVH Specificities

After creating a new VPS on OVH, you will receive an email with a link to the
password of the `debian` user on the machine. You will be prompted to change
the password on first login. After rebooting on NixOS, login as `root` instead.

IPv6 is not enabled by default on OVH VPSs.

```
nix-channel --add https://nixos.org/channels/nixos-24.05 nixos
nix-channel --update
```

Configure the IPv6 networking in `/etc/nixos/configuration.nix` with the data
from the OVH control panel.

```nix
networking.interfaces.ens3 = {
  ipv6.addresses = [ {
    address = "<IPv6 address>";
    prefixLength = 128;
  } ];
  defaultGateway6 = {
    address = "";
    interface = "ens3";
  };
};
```

#### DigitalOcean Specificities

When creating the droplet, make sure to select your SSH key, IPv6 networking
and to provide the Cloud-init script that will run `nixos-infect` on first boot,
automatically converting the system to NixOS.

```yaml
#cloud-config

runcmd:
  - curl https://raw.githubusercontent.com/elitak/nixos-infect/master/nixos-infect | PROVIDER=digitalocean NIX_CHANNEL=nixos-24.05 bash 2>&1 | tee /tmp/infect.log```
```

#### Scaleway Specificities

When creating an instance, specify the following Cloud-Init configuration ([source](https://wiki.nixos.org/wiki/Install_NixOS_on_Scaleway_X86_Virtual_Cloud_Server)):

```yaml
#cloud-config
write_files:
- path: /etc/nixos/host.nix
  permissions: '0644'
  content: |
    {pkgs, ...}:
    {
      environment.systemPackages = with pkgs; [ neofetch vim helix ];
    }
runcmd:
  - curl https://raw.githubusercontent.com/elitak/nixos-infect/master/nixos-infect |  NIXOS_IMPORT=./host.nix NIX_CHANNEL=nixos-24.05 bash 2>&1 | tee /tmp/infect.log
```

#### Test locally

Test the configuration locally before deploying it using
[nixos-generators](https://github.com/nix-community/nixos-generators).

```shell
nixos-generate -f vm -c vm.nix --run
```

Start the service manually to test it and look at the logs using:
```shell
systemctl start aleph-scoring-measure.service
journalctl -u aleph-scoring-measure.service
```

### Setting up the secret key

The key is in the format of `aleph address export-private-key`.

```shell
aleph address export-private-key > <key-path>
ssh <server> mkdir -p /srv/secrets/aleph.im
scp <key-path> <server>:/srv/secrets/aleph.im/ethereum.key
```

### Setting up the Sentry DSN

The Sentry DSN is the URL provided by Sentry to receive stacktraces.

```shell
echo "https://<...>@<...>.ingest.sentry.io/<...>" > sentry-dsn.txt
scp sentry-dsn.txt <server>:/srv/secrets/aleph.im/sentry-dsn.txt
```

### Configuring or Updating

The `service.nix` contains the aleph.im specific configuration for the metrics
service and a Prometheus exporter.

```shell
scp nix/service.nix <server>:/etc/nixos/service.nix
scp nix/aleph-scoring.nix <server>:/etc/nixos/aleph-scoring.nix
```

Ensure this file is imported by `/etc/nixos/configuration.nix` or `/etc/nixos/host.nix`
on each server.

Then switch to the new configuration using.

```shell
ssh <server> nixos-rebuild switch
```
