-- Per-CRN liveness over a short window (DEAD_NODE_WINDOW).
--
-- `proof` is true when a measurement demonstrates the node is a real CRN:
-- it answered the diagnostic VM (IPv6 ping or HTTP), served its own node_hash,
-- or returned a valid aleph-vm version. Ping and node_hash are unspoofable; the
-- diagnostic-VM HTTP and version signals are weakly spoofable.
--
-- $1 owner, $2 post type, $3 window start, $4 window end.
WITH proofs AS (
    SELECT
        node ->> 'node_id' AS node_id,
        date_trunc('hour', posts.creation_datetime) AS hour,
        (
            (node ->> 'diagnostic_vm_ping_latency') IS NOT NULL
            OR (node ->> 'diagnostic_vm_latency') IS NOT NULL
            OR COALESCE(
                (node ->> 'config_node_hash') = (node ->> 'node_id'), false
            )
            OR is_version_valid(
                'aleph-vm',
                node ->> 'version',
                to_timestamp((node ->> 'measured_at')::float)::date
            ) = 1
        ) AS proof
    FROM posts,
         jsonb_array_elements(content -> 'metrics' -> 'crn') node
    WHERE owner = $1
      AND type = $2
      AND posts.creation_datetime >= $3::timestamp
      AND posts.creation_datetime < $4::timestamp
),
per_hour AS (
    SELECT node_id, hour, bool_or(proof) AS proof
    FROM proofs
    GROUP BY node_id, hour
)
SELECT
    node_id,
    COALESCE(bool_or(proof), false) AS has_recent_proof,
    -- proof state of the most recent hour, used to split active vs inactive.
    COALESCE((array_agg(proof ORDER BY hour DESC))[1], false) AS latest_proof
FROM per_hour
GROUP BY node_id;
