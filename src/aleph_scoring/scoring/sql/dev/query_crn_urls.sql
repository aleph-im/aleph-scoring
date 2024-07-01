SELECT crn -> 'node_id',
       crn -> 'url'
FROM posts,
     jsonb_array_elements(content -> 'metrics' -> 'crn') crn
WHERE owner = '0x4d741d44348B21e97000A8C9f07Ee34110F7916F'
  AND type = 'test-aleph-scoring-metrics'
GROUP BY crn -> 'node_id',
         crn -> 'url'
LIMIT 1000
