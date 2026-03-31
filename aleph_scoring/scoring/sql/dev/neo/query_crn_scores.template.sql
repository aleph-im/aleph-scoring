WITH base_query AS (
    SELECT
        node ->> 'node_id' AS node_id,
        date_trunc('hour', posts.creation_datetime) AS hour,
        count((node -> 'base_latency')::float) AS base_latency_count,

        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) AS base_latency_67th_percentile,
        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'diagnostic_vm_latency')::float) AS diagnostic_vm_latency_67th_percentile,
        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'full_check_latency')::float) AS full_check_latency_67th_percentile,
        MAX(is_version_valid('aleph-vm', node ->> 'version', to_timestamp((node ->> 'measured_at')::float)::date)) AS version_valid
    FROM posts,
         jsonb_array_elements(content -> 'metrics' -> 'crn') node
    WHERE owner = $5
      AND type = $6
      AND posts.creation_datetime >= $2::timestamp
      AND posts.creation_datetime < $1::timestamp
      --AND node ->> 'node_id' IN ()
    GROUP BY node ->> 'node_id', date_trunc('hour', posts.creation_datetime)
),
ranked_query AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY hour DESC) AS recency_rank
    FROM base_query
)
SELECT
    node_id,
    SUM(version_valid) as valid_versions,
    COUNT(*) as record_count,
    (
        SUM(
            (
                geometric_pmf($3, recency_rank::INT) * $7
                +
                geometric_pmf($4, recency_rank::INT) * $8
            ) * (
                (
                    GREATEST(1 - ((base_latency_67th_percentile ^ 2) / 4), 0) *
                    GREATEST(1 - ((diagnostic_vm_latency_67th_percentile ^ 2) / 4), 0) *
                    GREATEST(1 - ((full_check_latency_67th_percentile ^ 2) / 40), 0)
                ) ^ (1/3.0)
            ) * (
                version_valid
            )
        )
    ) ^ 0.75 AS total_score
FROM ranked_query
GROUP BY node_id
ORDER BY total_score DESC;
