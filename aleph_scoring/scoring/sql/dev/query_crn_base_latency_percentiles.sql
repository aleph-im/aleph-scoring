SELECT node -> 'node_id'                                                             as node_id,
       percentile_cont(0.05) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_05,
       percentile_cont(0.10) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_10,
       percentile_cont(0.15) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_15,
       percentile_cont(0.20) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_20,
       percentile_cont(0.25) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_25,
       percentile_cont(0.30) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_30,
       percentile_cont(0.35) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_35,
       percentile_cont(0.40) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_40,
       percentile_cont(0.45) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_45,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_50,
       percentile_cont(0.55) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_55,
       percentile_cont(0.60) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_60,
       percentile_cont(0.65) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_65,
       percentile_cont(0.70) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_70,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_75,
       percentile_cont(0.80) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_80,
       percentile_cont(0.85) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_85,
       percentile_cont(0.90) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_90,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_95,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY (node -> 'base_latency')::float) as base_latency_98
FROM posts,
     jsonb_array_elements(content -> 'metrics' -> 'crn') node
WHERE owner = '0x0b92C8f4603Efb20d90008ec679B12Fbbf57Fec9'
  AND type = 'test-aleph-scoring-metrics'
GROUP BY node -> 'node_id'
LIMIT 1000