WITH publisher_hourly AS (
    SELECT
        node ->> 'node_id' AS node_id,
        COALESCE(content -> 'metrics' ->> 'server', owner) AS publisher,
        date_trunc('hour', posts.creation_datetime) AS hour,
        count((node -> 'base_latency')::float) AS base_latency_count,

        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) AS base_latency_67th_percentile,
        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'metrics_latency')::float) AS metrics_latency_67th_percentile,
        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'aggregate_latency')::float) AS aggregate_latency_67th_percentile,
        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'file_download_latency')::float) AS file_download_latency_67th_percentile,

        MAX(is_version_valid('pyaleph', node ->> 'version', to_timestamp((node ->> 'measured_at')::float)::date)) AS version_valid
    FROM posts,
         jsonb_array_elements(content -> 'metrics' -> 'ccn') node
    WHERE owner = ANY($5::text[])
      AND type = $6
      AND posts.creation_datetime >= $2::timestamp
      AND posts.creation_datetime < $1::timestamp
    GROUP BY node ->> 'node_id',
             COALESCE(content -> 'metrics' ->> 'server', owner),
             date_trunc('hour', posts.creation_datetime)
),
publisher_distance AS (
    SELECT
        node_id,
        publisher,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY base_latency_67th_percentile) AS median_base_latency
    FROM publisher_hourly
    WHERE base_latency_67th_percentile IS NOT NULL
      AND hour >= date_trunc('hour', $1::timestamp) - interval '7 days'
    GROUP BY node_id, publisher
),
publisher_ranked AS (
    SELECT
        node_id,
        publisher,
        ROW_NUMBER() OVER (
            PARTITION BY node_id
            ORDER BY median_base_latency ASC, publisher ASC
        ) AS publisher_rank
    FROM publisher_distance
),
publisher_candidates AS (
    SELECT
        publisher_hourly.*,
        publisher_ranked.publisher_rank,
        (
            publisher_hourly.base_latency_67th_percentile IS NOT NULL
            AND publisher_hourly.metrics_latency_67th_percentile IS NOT NULL
            AND publisher_hourly.aggregate_latency_67th_percentile IS NOT NULL
            AND publisher_hourly.file_download_latency_67th_percentile IS NOT NULL
        ) AS complete
    FROM publisher_hourly
    LEFT JOIN publisher_ranked
        ON publisher_hourly.node_id = publisher_ranked.node_id
       AND publisher_hourly.publisher = publisher_ranked.publisher
),
selected_hours AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY node_id, hour
            ORDER BY complete DESC,
                     COALESCE(publisher_rank, 2147483647) ASC,
                     publisher ASC
        ) AS publisher_selection_rank
    FROM publisher_candidates
),
ranked_query AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY hour DESC) AS recency_rank
    FROM selected_hours
    WHERE publisher_selection_rank = 1
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
                    GREATEST(1 - ((metrics_latency_67th_percentile ^ 2) / 4), 0) *
                    GREATEST(1 - ((aggregate_latency_67th_percentile ^ 2) / 8), 0) *
                    GREATEST(1 - ((file_download_latency_67th_percentile ^ 2) / 8), 0)
                ) ^ (1/4.0)
            ) * (
                version_valid
            )
        )
    ) ^ 0.75 AS total_score
FROM ranked_query
GROUP BY node_id
ORDER BY total_score DESC;
