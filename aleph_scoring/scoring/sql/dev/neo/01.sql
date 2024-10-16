WITH base_query AS (
    SELECT
        node ->> 'node_id' AS node_id,
        date_trunc('hour', posts.creation_datetime) AS hour,
        count((node -> 'base_latency')::float) AS base_latency_count,
        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) AS base_latency_67th_percentile,
        extract(epoch from age('2024-10-10T00:00'::timestamp, date_trunc('hour', posts.creation_datetime))) / 3600 AS hours_difference
    FROM posts,
         jsonb_array_elements(content -> 'metrics' -> 'crn') node
    WHERE owner = '0x4D52380D3191274a04846c89c069E6C3F2Ed94e4'
      AND type = 'aleph-network-metrics'
      AND posts.creation_datetime >= '2024-10-08T00:00'::timestamp
      AND posts.creation_datetime < '2024-10-10'::timestamp
      AND node ->> 'node_id' = '003d7057a82556ed3e49ca754f134999da6f0d36095d9a71b4380a8fac6ec041'
    GROUP BY node ->> 'node_id', date_trunc('hour', posts.creation_datetime)
)
SELECT
    node_id,
    hour,
    base_latency_count,
    base_latency_67th_percentile,
    -- Max 2 seconds, non-linear scoring
    1 - (base_latency_67th_percentile ^ 2) / 2 as score,
    hours_difference,
    -- hours_difference * (1 - (base_latency_67th_percentile ^ 2) / 2) AS hours_score
    EXP(-1 * (hours_difference / 48)) / 48 as decay
FROM base_query;
