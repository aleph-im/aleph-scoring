-- Per-CRN IPv4/IPv6 stability over a detection window.
--
-- One representative IP per node per hour (mode) absorbs round-robin / geo-DNS
-- noise, then LAG counts real address transitions (nulls ignored, so transient
-- resolution failures do not count as a change).
--
-- $1 owner, $2 post type, $3 window start, $4 window end.
WITH ip_observations AS (
    SELECT
        node ->> 'node_id' AS node_id,
        date_trunc('hour', posts.creation_datetime) AS hour,
        node ->> 'ipv4' AS ipv4,
        node ->> 'ipv6' AS ipv6
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
        mode() WITHIN GROUP (ORDER BY ipv6) AS ipv6
    FROM ip_observations
    GROUP BY node_id, hour
),
ordered AS (
    SELECT
        node_id,
        ipv4,
        ipv6,
        LAG(ipv4) OVER (PARTITION BY node_id ORDER BY hour) AS prev_ipv4,
        LAG(ipv6) OVER (PARTITION BY node_id ORDER BY hour) AS prev_ipv6
    FROM per_hour
)
SELECT
    node_id,
    bool_or(ipv4 IS NOT NULL) AS has_ipv4,
    bool_or(ipv6 IS NOT NULL) AS has_ipv6,
    count(*) FILTER (
        WHERE prev_ipv4 IS NOT NULL AND ipv4 IS NOT NULL AND ipv4 <> prev_ipv4
    ) AS ipv4_changes,
    count(*) FILTER (
        WHERE prev_ipv6 IS NOT NULL AND ipv6 IS NOT NULL AND ipv6 <> prev_ipv6
    ) AS ipv6_changes
FROM ordered
GROUP BY node_id;
