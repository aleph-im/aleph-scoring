SELECT crn -> 'node_id',
       percentile_cont(0.9) WITHIN GROUP (ORDER BY COALESCE((crn -> 'base_latency')::float, 100.)) as base_latency,
       percentile_cont(0.9) WITHIN GROUP (ORDER BY (crn -> 'base_latency')::float)                 as full_check_latency
FROM posts,
     jsonb_array_elements(content -> 'metrics' -> 'crn') crn
WHERE owner = '0x4d741d44348B21e97000A8C9f07Ee34110F7916F'
  AND type = 'test-aleph-scoring-metrics'
GROUP BY crn -> 'node_id'
LIMIT 10