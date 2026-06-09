-- Per-CRN IPv4/IPv6 stability over a detection window.
--
-- One representative IP per node per hour (mode) absorbs round-robin / geo-DNS
-- noise, then LAG counts real address transitions (nulls ignored, so transient
-- resolution failures do not count as a change).
--
-- IPv6 is compared at the /64 prefix only: CRNs are assigned a routed /64, so
-- the host bits may legitimately rotate (privacy / per-VM addresses) without
-- the node having moved. Only a /64 change is a real relocation.
--
-- $1 owner, $2 post type, $3 window start, $4 window end.
WITH ip_observations AS (
    SELECT
        node ->> 'node_id' AS node_id,
        date_trunc('hour', posts.creation_datetime) AS hour,
        node ->> 'ipv4' AS ipv4,
        CASE
            WHEN node ->> 'ipv6' IS NOT NULL AND node ->> 'ipv6' <> ''
            THEN network(set_masklen((node ->> 'ipv6')::inet, 64))::text
            ELSE NULL
        END AS ipv6_prefix
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
        mode() WITHIN GROUP (ORDER BY ipv6_prefix) AS ipv6_prefix
    FROM ip_observations
    GROUP BY node_id, hour
),
ordered AS (
    SELECT
        node_id,
        hour,
        ipv4,
        ipv6_prefix,
        LAG(ipv4) OVER (PARTITION BY node_id ORDER BY hour) AS prev_ipv4,
        LAG(ipv6_prefix) OVER (PARTITION BY node_id ORDER BY hour) AS prev_ipv6_prefix
    FROM per_hour
)
SELECT
    node_id,
    bool_or(ipv4 IS NOT NULL) AS has_ipv4,
    bool_or(ipv6_prefix IS NOT NULL) AS has_ipv6,
    count(*) FILTER (
        WHERE prev_ipv4 IS NOT NULL AND ipv4 IS NOT NULL AND ipv4 <> prev_ipv4
    ) AS ipv4_changes,
    count(*) FILTER (
        WHERE prev_ipv6_prefix IS NOT NULL
          AND ipv6_prefix IS NOT NULL
          AND ipv6_prefix <> prev_ipv6_prefix
    ) AS ipv6_changes,
    -- Most-recent non-null address, used to group duplicate CRNs.
    (array_agg(ipv4 ORDER BY hour DESC) FILTER (WHERE ipv4 IS NOT NULL))[1]
        AS current_ipv4,
    (array_agg(ipv6_prefix ORDER BY hour DESC) FILTER (WHERE ipv6_prefix IS NOT NULL))[1]
        AS current_ipv6_prefix
FROM ordered
GROUP BY node_id;
