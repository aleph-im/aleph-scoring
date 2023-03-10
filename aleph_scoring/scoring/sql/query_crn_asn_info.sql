SELECT node ->> 'node_id'                         as node_id,
       node -> 'asn'                              as asn,

       COUNT(*) OVER ()                           as total_nodes,
       COUNT(*) OVER (PARTITION BY node -> 'asn') AS nodes_with_identical_asn

FROM posts,
     jsonb_array_elements(content -> 'metrics' -> 'crn') node
WHERE owner = ANY (ARRAY ['0x4d741d44348B21e97000A8C9f07Ee34110F7916F', '0x4d741d44348B21e97000A8C9f07Ee34110F7916F'])
  AND type = 'aleph-scoring-metrics'
  AND to_timestamp((node -> 'measured_at')::float)::date > '2022-02-01'::date
  AND to_timestamp((node -> 'measured_at')::float)::date < '2023-03-10'::date
GROUP BY node ->> 'node_id',
         node -> 'asn'
ORDER BY node ->> 'node_id'
