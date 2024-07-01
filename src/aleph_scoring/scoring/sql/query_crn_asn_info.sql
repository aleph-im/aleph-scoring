SELECT node ->> 'node_id'                         as node_id,
       node -> 'asn'                              as asn,

       COUNT(*) OVER ()                           as total_nodes,
       COUNT(*) OVER (PARTITION BY node -> 'asn') AS nodes_with_identical_asn

FROM posts,
     jsonb_array_elements(content -> 'metrics' -> 'crn') node
WHERE owner = ANY (ARRAY ['0x4D52380D3191274a04846c89c069E6C3F2Ed94e4'])
  AND type = 'test-aleph-scoring-metrics'
  AND to_timestamp((node -> 'measured_at')::float)::timestamp > '2022-02-01'::timestamp
  AND to_timestamp((node -> 'measured_at')::float)::timestamp < '2023-04-10'::timestamp
GROUP BY node ->> 'node_id',
         node -> 'asn'
ORDER BY node ->> 'node_id'
