SELECT
    crn->>'node_id' as node_id,
    crn->'asn' as asn,

    COUNT(*) OVER () as total_nodes,
    COUNT(*) OVER (PARTITION BY crn->'asn') AS nodes_with_identical_asn

FROM
    posts, jsonb_array_elements(content->'metrics'->'crn') crn
WHERE
    owner = $1
    AND type = 'aleph-scoring-metrics'
    AND to_timestamp((crn->'measured_at')::float)::date > $2::date
    AND to_timestamp((crn->'measured_at')::float)::date < $3::date
GROUP BY
    crn->>'node_id',
    crn->'asn'
ORDER BY
    crn->>'node_id'
