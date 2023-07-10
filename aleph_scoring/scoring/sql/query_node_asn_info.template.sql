SELECT node ->> 'node_id'                         as node_id,
       node -> 'asn'                              as asn,

       COUNT(*) OVER ()                           as total_nodes,
       COUNT(*) OVER (PARTITION BY node -> 'asn') AS nodes_with_identical_asn

FROM posts,
     jsonb_array_elements(content -> 'metrics' -> $4) node
WHERE owner = $1
  AND type = $5
  AND to_timestamp((node -> 'measured_at')::float)::timestamp > $2::timestamp
  AND to_timestamp((node -> 'measured_at')::float)::timestamp < $3::timestamp
GROUP BY node ->> 'node_id',
         node -> 'asn'
ORDER BY node ->> 'node_id'
