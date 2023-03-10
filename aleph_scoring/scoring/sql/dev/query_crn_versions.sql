SELECT node -> 'node_id'  as node_id,
       node ->> 'version' as version
FROM posts,
     jsonb_array_elements(content -> 'metrics' -> 'crn') node
WHERE owner = '0x0b92C8f4603Efb20d90008ec679B12Fbbf57Fec9'
  AND type = 'test-aleph-scoring-metrics'
  AND node ->> 'version' is not null
GROUP BY node -> 'node_id',
         node ->> 'version'
LIMIT 1000