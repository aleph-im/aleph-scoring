WITH base_query AS (
    SELECT
        node ->> 'node_id' AS node_id,
        date_trunc('hour', posts.creation_datetime) AS hour,
        count((node -> 'base_latency')::float) AS base_latency_count,

        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) AS base_latency_67th_percentile,
        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'metrics_latency')::float) AS metrics_latency_67th_percentile,
        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'aggregate_latency')::float) AS aggregate_latency_67th_percentile,
        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'file_download_latency')::float) AS file_download_latency_67th_percentile,

        MAX(is_version_valid('pyaleph', node ->> 'version', to_timestamp((node ->> 'measured_at')::float)::date)) AS version_valid,

        EXTRACT(EPOCH FROM AGE($1::timestamp, date_trunc('hour', posts.creation_datetime))) / 3600 AS hours_difference
    FROM posts,
         jsonb_array_elements(content -> 'metrics' -> 'ccn') node
    WHERE owner = $5
      AND type = $6
      AND posts.creation_datetime >= $2::timestamp
      AND posts.creation_datetime < $1::timestamp
      --AND node ->> 'node_id' IN ()
    GROUP BY node ->> 'node_id', date_trunc('hour', posts.creation_datetime)
)
SELECT
    node_id,
    SUM(version_valid) as valid_versions,
    COUNT(*) as record_count,
    (
        SUM(
            (
                geometric_pmf($3, CEIL(hours_difference)) * $7
                +
                geometric_pmf($4, CEIL(hours_difference)) * $8
            ) * (
                (
                    GREATEST(1 - ((base_latency_67th_percentile ^ 2) / 4), 0) *
                    GREATEST(1 - ((metrics_latency_67th_percentile ^ 2) / 4), 0) *
                    GREATEST(1 - ((aggregate_latency_67th_percentile ^ 2) / 8), 0) *
                    GREATEST(1 - ((file_download_latency_67th_percentile ^ 2) / 8), 0)
                ) ^ (1/4.0)
            ) * (
                version_valid
            )
        ) ^ 0.75
    ) AS total_score
FROM base_query
GROUP BY node_id
ORDER BY total_score DESC;