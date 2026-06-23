-- Per-CRN IPv4/IPv6 stability over a detection window.
--
-- One representative IP per node per hour (mode) absorbs round-robin / geo-DNS
-- noise, then LAG counts real address transitions (nulls ignored, so transient
-- resolution failures do not count as a change).
--
-- IPv6 stability tracks the node's VM IPv6 pool (networking.IPV6_ADDRESS_POOL
-- from /status/config), not the node's own access IPv6 (the AAAA record). Some
-- operators give the node a default IPv6 for access and route a separate range
-- for VMs, so the pool is the address that actually identifies the node. The
-- pool is normalised to its network/prefix so non-canonical host bits do not
-- count as a change.
--
-- $1 owner, $2 post type, $3 window start, $4 window end.
WITH ip_observations AS (
    SELECT
        node ->> 'node_id' AS node_id,
        date_trunc('hour', posts.creation_datetime) AS hour,
        node ->> 'ipv4' AS ipv4,
        CASE
            WHEN node ->> 'ipv6_pool' IS NOT NULL AND node ->> 'ipv6_pool' <> ''
            THEN network((node ->> 'ipv6_pool')::inet)::text
            ELSE NULL
        END AS ipv6_pool,
        -- The node served its own registered hash at /status/config.
        ((node ->> 'config_node_hash') = (node ->> 'node_id')) AS verified
    FROM posts,
         jsonb_array_elements(content -> 'metrics' -> 'crn') node
    WHERE owner = $1
      AND type = $2
      AND posts.creation_datetime >= $3::timestamp
      AND posts.creation_datetime < $4::timestamp
),
per_hour AS (
    SELECT
        node_id,
        hour,
        mode() WITHIN GROUP (ORDER BY ipv4) AS ipv4,
        mode() WITHIN GROUP (ORDER BY ipv6_pool) AS ipv6_pool,
        bool_or(verified) AS verified
    FROM ip_observations
    GROUP BY node_id, hour
),
ordered AS (
    SELECT
        node_id,
        hour,
        ipv4,
        ipv6_pool,
        verified,
        LAG(ipv4) OVER (PARTITION BY node_id ORDER BY hour) AS prev_ipv4,
        LAG(ipv6_pool) OVER (PARTITION BY node_id ORDER BY hour) AS prev_ipv6_pool
    FROM per_hour
)
SELECT
    node_id,
    bool_or(ipv4 IS NOT NULL) AS has_ipv4,
    bool_or(ipv6_pool IS NOT NULL) AS has_ipv6,
    count(*) FILTER (
        WHERE prev_ipv4 IS NOT NULL AND ipv4 IS NOT NULL AND ipv4 <> prev_ipv4
    ) AS ipv4_changes,
    count(*) FILTER (
        WHERE prev_ipv6_pool IS NOT NULL
          AND ipv6_pool IS NOT NULL
          AND ipv6_pool <> prev_ipv6_pool
    ) AS ipv6_changes,
    -- Most-recent non-null address, used to group duplicate CRNs.
    (array_agg(ipv4 ORDER BY hour DESC) FILTER (WHERE ipv4 IS NOT NULL))[1]
        AS current_ipv4,
    (array_agg(ipv6_pool ORDER BY hour DESC) FILTER (WHERE ipv6_pool IS NOT NULL))[1]
        AS current_ipv6_pool,
    -- True if the node ever served its own hash in the window.
    coalesce(bool_or(verified), false) AS verified
FROM ordered
GROUP BY node_id;
