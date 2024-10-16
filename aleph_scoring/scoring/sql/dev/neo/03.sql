WITH base_query AS (
    SELECT
        node ->> 'node_id' AS node_id,
        date_trunc('hour', posts.creation_datetime) AS hour,
        count((node -> 'base_latency')::float) AS base_latency_count,
        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) AS base_latency_67th_percentile,
        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'diagnostic_vm_latency')::float) AS diagnostic_vm_latency_67th_percentile,
        percentile_cont(0.67) WITHIN GROUP (ORDER BY (node -> 'full_check_latency')::float) AS full_check_latency_67th_percentile,

        EXTRACT(EPOCH FROM AGE('2024-10-15T12:00'::timestamp, date_trunc('hour', posts.creation_datetime))) / 3600 AS hours_difference
    FROM posts,
         jsonb_array_elements(content -> 'metrics' -> 'crn') node
    WHERE owner = '0x4D52380D3191274a04846c89c069E6C3F2Ed94e4'
      AND type = 'aleph-network-metrics'
      AND posts.creation_datetime >= '2024-08-01T00:00'::timestamp
      AND posts.creation_datetime < '2024-10-15T12:00'::timestamp
      AND node ->> 'node_id' IN (
          '45c5daf617434c021863a825591756ace6ff4e8ffc5b898e190a78f4894a4781',
          '95fa9b2aee1d6622962c8b0db01a30445e72d8a40eb4f330ea51d98aa377ee18',
          'adb466cdf6881b2344d5cb443945e25862fe65fbe329f7f7255addc618bfcdb1',
          '4f0eccb626a659d6950ff7b18df19c3fbf70a19acc2949478e31d593a6240208',
          '043b0b086b21020d17b4e5fc520095fb53083d759119bf61a1b7c116cdd7e7b2'
      )
    GROUP BY node ->> 'node_id', date_trunc('hour', posts.creation_datetime)
)
SELECT
    node_id,
    hour,
    base_latency_count,
    base_latency_67th_percentile,
    1 - (base_latency_67th_percentile ^ 2) / 2 AS base_latency_score,
    hours_difference,

    (
        geometric_pmf(%(p1)s, CEIL(hours_difference)) * %(p1_ratio)s
        +
        geometric_pmf(%(p2)s, CEIL(hours_difference)) * %(p2_ratio)s
    ) * GREATEST(1 - ((base_latency_67th_percentile ^ 2) / 4), 0)
      * GREATEST(1 - ((diagnostic_vm_latency_67th_percentile ^ 2) / 4), 0)
      * GREATEST(1 - ((full_check_latency_67th_percentile ^ 2) / 20), 0)

    AS score
FROM base_query
ORDER BY hours_difference;