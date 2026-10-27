# Deploying the metrics service as a Proxmox LXC container

This builds a NixOS LXC template from the flake that runs the metrics
measurement publisher: every hour it runs `scoring measure --publish` and
publishes an `aleph-network-metrics` post signed with the configured key. It
does not compute scores (`scoring compute-scores` runs elsewhere).

Each container is one metrics vantage point; create one per measurement
location/provider, all signing with the trusted key.

Images are self-contained: the `aleph-scoring` package is installed into the
Nix store inside the image, so no repository checkout is needed in the
container. The project directory itself (docs, helper scripts, `.env`, tests)
is not part of the image.

Files involved:

- `nix/service.nix` - NixOS module (systemd service + timer, env options)
- `nix/proxmox-lxc.nix` - LXC configuration (imports the module above)
- `flake.nix` - exposes `nixosConfigurations.proxmox-lxc`

The secrets file and environment file are **not** baked into the image. They are
pushed into the container after creation and can be changed at runtime without
rebuilding anything.

## 1. Build the template

On a machine with Nix (the Proxmox host works, or any build host):

```sh
git clone https://github.com/aleph-im/aleph-scoring
cd aleph-scoring
nix build .#nixosConfigurations.proxmox-lxc.config.system.build.tarball
```

The tarball is at `result/tarball/nixos-system-<system>.tar.xz` (on x86_64:
`result/tarball/nixos-system-x86_64-linux.tar.xz`). Copy it into the Proxmox
template cache with the name you will use with `pct create`:

```sh
scp result/tarball/nixos-system-x86_64-linux.tar.xz \
  root@proxmox:/var/lib/vz/template/cache/nixos-aleph-metrics.tar.xz
```

On the Proxmox host, verify the storage exposes `vztmpl` and the file is
listed:

```sh
pvesm status --content vztmpl
pvesm list local --content vztmpl | grep aleph-metrics
```

## 2. Create the container

On the Proxmox host:

```sh
pct create 200 local:vztmpl/nixos-aleph-metrics.tar.xz \
  --hostname aleph-metrics \
  --unprivileged 1 --features nesting=1,keyctl=1 \
  --cores 2 --memory 2048 --swap 2048 \
  --rootfs local-lvm:8 \
  --net0 name=eth0,bridge=vmbr0,ip=dhcp \
  --ssh-public-keys /root/.ssh/authorized_keys
pct start 200
```

`nesting=1,keyctl=1` is required: without it the Nix sandbox and some services
fail in an unprivileged container.

## 3. Push the secrets and environment

The Ethereum key (the trusted `0x2896...` key) and the optional env overrides:

```sh
pct push 200 ./ethereum.key /srv/secrets/aleph.im/ethereum.key --perms 600

cat > /tmp/aleph-scoring.env <<'EOF'
# Optional runtime overrides for the measurement daemon.
# ALEPH_SCORING_LOGGING_LEVEL=DEBUG
# ALEPH_SCORING_LOGGING_LEVEL=INFO
EOF
pct push 200 /tmp/aleph-scoring.env /etc/aleph-scoring.env --perms 600
```

Run one measurement and check the logs:

```sh
pct exec 200 -- systemctl start aleph-scoring-measure
pct exec 200 -- journalctl -u aleph-scoring-measure -n 50 --no-pager
```

The timer then runs the service every hour (`OnCalendar=*-*-* *:00:00`,
`Persistent=true`).

## Verify the measurement publisher is running

```sh
pct exec 200 -- systemctl list-timers aleph-scoring-measure.timer
pct exec 200 -- systemctl status aleph-scoring-measure --no-pager
pct exec 200 -- journalctl -u aleph-scoring-measure -f
```

A successful run logs `Published metrics on Aleph with status ...` with an item
hash. Confirm the post is attributed to the trusted sender:

```sh
pct exec 200 -- journalctl -u aleph-scoring-measure | grep Published
```

## Changing the key or environment later

No rebuild is needed. The key file is read at every run and the environment
file is read by systemd at every service start:

```sh
pct push 200 ./new-ethereum.key /srv/secrets/aleph.im/ethereum.key --perms 600
pct push 200 ./new.env /etc/aleph-scoring.env --perms 600
```

Values in `/etc/aleph-scoring.env` override `extraEnvironment` from the Nix
configuration (systemd `EnvironmentFile` wins over `Environment=`).

## Baked-in (non-secret) settings

Non-secret defaults live in `nix/proxmox-lxc.nix`:

```nix
services.aleph-scoring = {
  enable = true;
  ethereumKeyPath = "/srv/secrets/aleph.im/ethereum.key";
  environmentFile = "/etc/aleph-scoring.env";
};
```

Change them there and rebuild the template. Prefer the env file for anything
that changes often.

The container only measures and publishes metrics. Settings related to scoring
(`ALEPH_SCORING_ALLOWED_METRICS_SENDER`, `ALEPH_SCORING_SCORE_METRICS_PERIOD`,
`DATABASE_*`) are not used here; they belong on the host that runs
`scoring compute-scores` / `backfill_metrics.py`.

## Notes

- `nix/service.nix` no longer configures swap; `swapDevices` is DigitalOcean
  specific and lives in `nix/digitalocean.nix`.
- The Sentry DSN file is optional; the code checks `is_file()` before reading.
- `pct push` writes as root inside the container, which is what the service
  expects.
- To update the NixOS system later: rebuild the tarball and recreate the
  container, or copy the repo into the container and run
  `nixos-rebuild switch --flake .#proxmox-lxc`. Secrets and the env file live on
  the container disk and survive rebuilds.
