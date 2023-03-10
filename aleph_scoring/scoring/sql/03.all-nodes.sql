SELECT
    percentile_cont(0.25) WITHIN GROUP (ORDER BY (crn->'base_latency')::float) as base_latency_percentile_25,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY (crn->'base_latency')::float) as base_latency_percentile_50,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY (crn->'base_latency')::float) as base_latency_percentile_75,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY (crn->'base_latency')::float) as base_latency_percentile_95,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY (crn->'base_latency')::float) as base_latency_percentile_99

FROM
    posts, jsonb_array_elements(content->'metrics'->'crn') crn
WHERE
    owner = '0x4d741d44348B21e97000A8C9f07Ee34110F7916F'
    AND type = 'test-aleph-scoring-metrics'
LIMIT 10