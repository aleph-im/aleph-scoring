SELECT
    node->>'node_id' as node_id,
    node->'asn' as asn,

    COUNT(*) OVER () as total_nodes,
    COUNT(*) OVER (PARTITION BY node->'asn') AS nodes_with_identical_asn

FROM
    posts, jsonb_array_elements(content->'metrics'->'ccn') node
WHERE
    owner = $1
    AND type = 'aleph-scoring-metrics'
    AND to_timestamp((node->'measured_at')::float)::date > $2::date
    AND to_timestamp((node->'measured_at')::float)::date < $3::date
GROUP BY
    node->>'node_id',
    node->'asn'
ORDER BY
    node->>'node_id'
