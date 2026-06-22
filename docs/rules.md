# Scoring Rules & Decision Impact

How node scores are computed, and every rule/condition that moves a score.

## How scoring works

Scoring is a stateless aggregation over the metrics that this tool publishes to
the Aleph network (post type `aleph-network-metrics`). For each scoring run, the
SQL queries read every metrics post in the window from the Aleph node's Postgres
`posts` table and aggregate them per node. All scores are floats in `[0, 1]`.

Two node types are scored independently: **CCN** (Core Channel Node) and **CRN**
(Compute Resource Node). Each node gets a `total_score` (performance) and a
separate `decentralization` value; both are published and combined downstream.

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `SCORE_METRICS_PERIOD` | 730 days | Window of metrics aggregated into a score |
| `MAX_METRICS_AGE` | 3 hours | If the newest metrics are older than this, the run aborts |
| `VERSION_GRACE_PERIOD` | 14 days | A replaced software version still counts as valid for this long |
| `IP_STABILITY_WINDOW` | 365 days | Window for the CRN IP-stability check |
| `IP_MAX_CHANGES` | 2 | IPv4/IPv6-/64 changes at or above this penalize the node |
| `IP_STABILITY_ENFORCED` | `false` | Whether the IP-stability penalty actually zeroes scores |
| `DUPLICATE_IP_ENFORCED` | `true` | Whether the duplicate-IP penalty actually zeroes scores |
| `DEAD_NODE_WINDOW` | 24 hours | Window in which a CRN must prove it is a real CRN |
| `DEAD_NODE_ENFORCED` | `false` | Whether dead nodes are actually forced to 0 |

All are overridable via `ALEPH_SCORING_<NAME>` env vars.

## Run-level preconditions

| Condition | Decision impact |
|---|---|
| Newest metrics older than `MAX_METRICS_AGE` | Entire scoring run aborts (exit code 2); no scores published |
| A node has no ASN info in the window | Node is **skipped** — it receives no score this run |

## CRN scoring

### 1. Performance score (`total_score`)

Computed in `sql/dev/neo/query_crn_scores.template.sql`. Per hour, the 67th
percentile of each latency is taken; per hour a quality factor is built, weighted
by recency, summed over the window, and raised to the power `0.75`.

Per-hour quality factor:

```
( f(base_latency)        # GREATEST(1 - base_latency^2 / 4,  0)   IPv6 about/login
* f(diagnostic_vm_latency)# GREATEST(1 - dvm_latency^2  / 4,  0)
* f(full_check_latency) )^(1/3)  # GREATEST(1 - full_check^2 / 40, 0)
* version_valid          # 0 or 1 (see below)
```

| Condition | Decision impact |
|---|---|
| Lower latencies | Higher quality factor → higher score |
| Any latency high enough that `1 - latency^2/k <= 0` | That latency's factor is 0 → the hour contributes 0 |
| `version_valid = 0` for an hour | That hour contributes 0 to the score |
| More recent measurements | Weighted more (geometric decay over recency rank) |

### 2. Version validity (`is_version_valid`)

Per measurement, the reported `aleph-vm` version is checked against the
`software_versions` table.

| Condition | Result |
|---|---|
| Version current at measurement time | `version_valid = 1` |
| Version replaced, but within `VERSION_GRACE_PERIOD` (14 days) | `version_valid = 1` |
| Version unknown or replaced longer ago | `version_valid = 0` → contributes 0 to score |

### 3. Decentralization (separate output)

```
decentralization = (1 - nodes_with_identical_asn / total_nodes) ^ 2
```

| Condition | Decision impact |
|---|---|
| Node alone in its ASN | `decentralization = 1` |
| Node shares its ASN with others | Lower, approaching 0 as the share grows |
| Every node in the same ASN | `decentralization = 0` |

Reported alongside `total_score`; not folded into it here.

### 4. IP-stability penalty

Over `IP_STABILITY_WINDOW`, a representative IP per node per hour is taken
(`mode()`), and address transitions are counted (`LAG`, nulls ignored). For
IPv6 the node's **VM address pool** (`networking.IPV6_ADDRESS_POOL` from
`/status/config`) is used, not the node's own access IPv6 (the AAAA record):
some operators give the node a default IPv6 for access and route a separate
range for VMs, so the pool is what identifies the node. The pool is normalised
to its network/prefix before comparison. Active only when
`IP_STABILITY_ENFORCED = true`.

| Condition (within window) | Decision impact | Issue code |
|---|---|---|
| No IPv4 observed | `total_score → 0` (when enforced) | 1001 `NO_IPV4` |
| No IPv6 pool observed | `total_score → 0` (when enforced) | 1002 `NO_IPV6` |
| IPv4 changed `>= IP_MAX_CHANGES` (2) times | `total_score → 0` (when enforced) | 1003 `IPV4_UNSTABLE` |
| IPv6 pool changed `>= IP_MAX_CHANGES` (2) times | `total_score → 0` (when enforced) | 1004 `IPV6_UNSTABLE` |
| No recorded IP history yet (new node) | No penalty (defaults non-penalizing) | — |

Scoring-reason codes are published whenever the condition holds, even while
`IP_STABILITY_ENFORCED = false`, so operators get early warning before scores
drop.

### 5. Duplicate-IP penalty

Each node's most-recent representative IPv4 and IPv6 VM pool are grouped across
all measured CRNs. For each address (IPv4 and IPv6 pool grouped independently),
only the **earliest-registered** node (node aggregate `time`) keeps its score.
Active only when `DUPLICATE_IP_ENFORCED = true`.

| Condition | Decision impact | Issue code |
|---|---|---|
| Shares its IPv4 with an older CRN | `total_score → 0` (when enforced) | 1005 `DUPLICATE_IP` |
| Shares its IPv6 pool with an older CRN | `total_score → 0` (when enforced) | 1005 `DUPLICATE_IP` |
| Earliest-registered node on the address | Kept — full score | — |
| Unique address | No penalty | — |

A node is penalized if it is not the earliest in its IPv4 cohort **or** its
IPv6-pool cohort. The code is published whenever the condition holds, even while
enforcement is off. Caveat: NAT/reverse-proxy setups can legitimately share an
IPv4 — observe the published codes before enabling enforcement.

### 6. Liveness status (active / inactive / dead)

Separately from the performance decay, each CRN gets a `status` (published
top-level on the score) based on whether it recently **proved it is a real CRN**
— answering the diagnostic VM (IPv6 ping or HTTP), serving its own `node_hash`,
or returning a valid aleph-vm version. This stops the gradual decay from leaving
a non-CRN (e.g. an IP re-assigned to a plain webserver) looking healthy to
schedulers for ~a week.

| Condition | Status | Decision impact | Issue code |
|---|---|---|---|
| Latest measurement proves CRN-ness | `active` | normal score | — |
| Proved within `DEAD_NODE_WINDOW` but not latest | `inactive` | normal score | 1007 `NODE_INACTIVE` |
| No proof for `DEAD_NODE_WINDOW` (24h) | `dead` | `total_score → 0`, `decentralization → 0` (when enforced) | 1006 `NODE_DEAD` |

`status` is **always** published so schedulers/load-balancers can avoid dead
nodes even before `DEAD_NODE_ENFORCED` is turned on. A node that keeps proving
CRN-ness still gets the full performance-decay grace; only *no proof for 24h*
triggers the hard 0. Ping and `node_hash` proofs are unspoofable; the
diagnostic-VM HTTP and version proofs are weakly spoofable.

## CCN scoring

Same structure as CRN, with CCN-specific latencies and the `pyaleph` version.

Per-hour quality factor:

```
( f(base_latency)            # 1 - base_latency^2 / 4
* f(metrics_latency)         # 1 - metrics_latency^2 / 4
* f(aggregate_latency)       # 1 - aggregate_latency^2 / 8
* f(file_download_latency) )^(1/4)  # 1 - file_download_latency^2 / 8
* version_valid
```

Decentralization is identical to CRN. The IP-stability penalty is **CRN-only**.

## Issue codes

Compact numeric codes published per node so operators can self-diagnose. Names
live in `aleph_scoring/issue_codes.py` (the source of truth); only the integers
appear in the JSON. **Values are a stability contract — never reused or
renumbered.**

### Scoring reasons (1xxx) — on `CrnScore.codes`

| Code | Name | Meaning |
|---|---|---|
| 1001 | `NO_IPV4` | No IPv4 observed in the stability window |
| 1002 | `NO_IPV6` | No IPv6 observed in the stability window |
| 1003 | `IPV4_UNSTABLE` | IPv4 address changed too many times |
| 1004 | `IPV6_UNSTABLE` | IPv6 VM pool changed too many times |
| 1005 | `DUPLICATE_IP` | Shares an IPv4 or IPv6 VM pool with an older CRN |
| 1006 | `NODE_DEAD` | No proof of being a CRN for 24h; treated as dead |
| 1007 | `NODE_INACTIVE` | Not currently proving it is a CRN |

### Per-measurement (2xxx) — on `CrnMetrics.codes`

| Code | Name | Meaning |
|---|---|---|
| 2001 | `DNS_IPV4_FAIL` | Hostname did not resolve to an IPv4 address |
| 2002 | `DNS_IPV6_FAIL` | Hostname did not resolve to an IPv6 address |
| 2003 | `IPV4_CHECK_FAILED` | IPv4 reachability check (about/login) failed |
| 2004 | `IPV6_CHECK_FAILED` | IPv6 reachability check (about/login) failed |
| 2005 | `VERSION_UNAVAILABLE` | aleph-vm version could not be read |
| 2006 | `DIAG_VM_UNREACHABLE` | Diagnostic VM did not respond |
| 2007 | `FULL_CHECK_FAILED` | status/check/fastapi probe failed |
| 2999 | `UNKNOWN_MEASUREMENT_ERROR` | Unexpected error while measuring; see node logs |
