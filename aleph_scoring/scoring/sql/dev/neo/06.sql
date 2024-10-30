WITH base_query AS (
    SELECT
        node ->> 'node_id' AS node_id,
        date_trunc('hour', posts.creation_datetime) AS hour,
        count((node -> 'base_latency')::float) AS base_latency_count,

        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) AS base_latency_67th_percentile,
        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'diagnostic_vm_latency')::float) AS diagnostic_vm_latency_67th_percentile,
        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'full_check_latency')::float) AS full_check_latency_67th_percentile,
        MAX(is_version_valid('aleph-vm', node ->> 'version', to_timestamp((node ->> 'measured_at')::float)::date)) AS version_valid,

        EXTRACT(EPOCH FROM AGE(%(to_date)s::timestamp, date_trunc('hour', posts.creation_datetime))) / 3600 AS hours_difference
    FROM posts,
         jsonb_array_elements(content -> 'metrics' -> 'crn') node
    WHERE owner = '0x4D52380D3191274a04846c89c069E6C3F2Ed94e4'
      AND type = 'aleph-network-metrics'
      AND posts.creation_datetime >= %(from_date)s::timestamp
      AND posts.creation_datetime < %(to_date)s::timestamp
      --AND node ->> 'node_id' IN ()
    GROUP BY node ->> 'node_id', date_trunc('hour', posts.creation_datetime)
)
SELECT
    node_id,
    SUM(version_valid) as valid_versions,
    SUM(
        (
            geometric_pmf(%(p1)s, CEIL(hours_difference)) * %(p1_ratio)s
            +
            geometric_pmf(%(p2)s, CEIL(hours_difference)) * %(p2_ratio)s
        ) * (
            (
                GREATEST(1 - ((base_latency_67th_percentile ^ 2) / 4), 0) *
                GREATEST(1 - ((diagnostic_vm_latency_67th_percentile ^ 2) / 4), 0) *
                GREATEST(1 - ((full_check_latency_67th_percentile ^ 2) / 40), 0)
            ) ^ (1/3.0)
        ) * (
            version_valid
        ) * (
            -- This multiplier moves the score of the large majority of nodes above 80%.
            1.1
        )
    ) AS total_score,
    COUNT(*) as record_count
FROM base_query
GROUP BY node_id
ORDER BY total_score DESC;