SELECT crn -> 'node_id'                                                             as node_id,

--     percentile_cont(0.25) WITHIN GROUP (ORDER BY (crn->'base_latency')::float) as base_latency_percentile_25,
--     percentile_cont(0.50) WITHIN GROUP (ORDER BY (crn->'base_latency')::float) as base_latency_percentile_50,
--     percentile_cont(0.75) WITHIN GROUP (ORDER BY (crn->'base_latency')::float) as base_latency_percentile_75,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY (crn -> 'base_latency')::float) as base_latency_percentile_95,
--     percentile_cont(0.99) WITHIN GROUP (ORDER BY (crn->'base_latency')::float) as base_latency_percentile_99,

       count((crn -> 'base_latency')::float > 0)                                    as value_present,
       count(case when (crn -> 'base_latency')::float is null then 1 end)           as value_missing,

    /* Compute the score as 1 - half of the base latency. The worst pings across Earth are around 600ms, so any
       decent server should be able to respond within 2 seconds and have a score. */
       greatest(
                   1 - percentile_cont(0.25) WITHIN GROUP (ORDER BY COALESCE((crn -> 'base_latency')::float, 100.)) / 2,
                   0
           )                                                                        as base_latency_score_p25,

    /* Compute the score as 1 - half of the base latency. The worst pings across Earth are around 600ms, so any
       decent server should be able to respond within 2 seconds and have a score. */
       greatest(
                   1 - percentile_cont(0.95) WITHIN GROUP (ORDER BY COALESCE((crn -> 'base_latency')::float, 100.)) / 2,
                   0
           )                                                                        as base_latency_score_p95,

    /*
        -log(x+1) + 1
        where x is the 95th percentile
        with a free allowance of 0.25
        and null values replaced with 100
    */
--     greatest(
--         -log(
--             percentile_cont(0.95) WITHIN GROUP (ORDER BY
--                 -- Coalesce null values to a very bad value
--                 greatest(
--                     COALESCE(
--                         (crn -> 'base_latency')::float - 0.25 /* allowance, approx. global percentile 75 */,
--                         100.
--                     ),
--                     0 /* avoid negative values due to allowance */
--                 )
--             )
--             + 1
--         ) + 1,
--         0
--     )
--     as base_latency_score_95,

       percentile_cont(0.9) WITHIN GROUP (ORDER BY (crn -> 'base_latency')::float)  as full_check_latency
FROM posts,
     jsonb_array_elements(content -> 'metrics' -> 'crn') crn
WHERE owner = '0x4d741d44348B21e97000A8C9f07Ee34110F7916F'
  AND type = 'test-aleph-scoring-metrics'
GROUP BY crn -> 'node_id'
ORDER BY base_latency_score_p95 DESC
LIMIT 10000