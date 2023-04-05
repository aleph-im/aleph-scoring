SELECT node ->> 'node_id'                                                  as node_id,

       count((node -> 'base_latency')::float > 0)                          as base_latency_present,
       count(case when (node -> 'base_latency')::float is null then 1 end) as base_latency_missing,

    /* Compute the score as 1 - half of the base latency. The worst pings across Earth are around 600ms, so any
       decent server should be able to respond within 2 seconds and have a score. */
       greatest(
                   1 -
                   percentile_disc(0.25) WITHIN GROUP (ORDER BY COALESCE((node -> 'base_latency')::float, 100.)) / 2,
                   0
           )                                                               as base_latency_score_p25,

    /* Compute the score as 1 - half of the base latency. The worst pings across Earth are around 600ms, so any
       decent server should be able to respond within 2 seconds and have a score. */
       greatest(
                   1 -
                   percentile_disc(0.95) WITHIN GROUP (ORDER BY COALESCE((node -> 'base_latency')::float, 100.)) / 2,
                   0
           )                                                               as base_latency_score_p95,

       greatest(
                   1 -
                   percentile_disc(0.25) WITHIN GROUP (ORDER BY COALESCE((node -> 'metrics_latency')::float, 100.)) /
                   2.5,
                   0
           )                                                               as metrics_latency_score_p25,

       greatest(
                   1 -
                   percentile_disc(0.95) WITHIN GROUP (ORDER BY COALESCE((node -> 'metrics_latency')::float, 100.)) /
                   2.5,
                   0
           )                                                               as metrics_latency_score_p95,

       greatest(
                   1 -
                   percentile_disc(0.25) WITHIN GROUP (ORDER BY COALESCE((node -> 'aggregate_latency')::float, 100.)) /
                   4,
                   0
           )                                                               as aggregate_latency_score_p25,

       greatest(
                   1 -
                   percentile_disc(0.95) WITHIN GROUP (ORDER BY COALESCE((node -> 'aggregate_latency')::float, 100.)) /
                   4,
                   0
           )                                                               as aggregate_latency_score_p95,

       greatest(
                   1 - percentile_disc(0.25)
                       WITHIN GROUP (ORDER BY COALESCE((node -> 'file_download_latency')::float, 100.)) / 4,
                   0
           )                                                               as file_download_latency_score_p25,

       greatest(
                   1 - percentile_disc(0.95)
                       WITHIN GROUP (ORDER BY COALESCE((node -> 'file_download_latency')::float, 100.)) / 4,
                   0
           )                                                               as file_download_latency_score_p95,

       greatest(least
                    (
                            1.5 - percentile_disc(0.25)
                                  WITHIN GROUP (ORDER BY COALESCE((node -> 'eth_height_remaining')::int, 1000.)) / 100.,
                            1
                    )
           , 0)                                                            as eth_height_remaining_score_p25,

       greatest(least
                    (
                            2 - percentile_disc(0.95)
                                WITHIN GROUP (ORDER BY COALESCE((node -> 'eth_height_remaining')::int, 1000.)) / 275.,
                            1
                    )
           , 0)                                                            as eth_height_remaining_score_p95,

       count(case when (node ->> 'version' = 'v0.4.4') then 1 end)         as node_version_latest,
       count(case
                 when (
                                 node ->> 'version' = 'v0.4.3' and
                                 to_timestamp((node -> 'measured_at')::float)::date <= '2022-10-20'::date
                     ) then 1 end)                                         as node_version_outdated,
       count(case
                 when (
                                 node ->> 'version' != 'v0.4.4' and
                                 to_timestamp((node -> 'measured_at')::float)::date > '2022-10-20'::date
                     ) then 1 end)                                         as node_version_obsolete,
       count(case when (coalesce(node ->> 'version', '') = '') then 1 end) as node_version_missing

FROM posts,
     jsonb_array_elements(content -> 'metrics' -> 'ccn') node
WHERE owner = ANY (ARRAY ['0x4D52380D3191274a04846c89c069E6C3F2Ed94e4', '0x4D52380D3191274a04846c89c069E6C3F2Ed94e4'])
  AND type = 'test-aleph-scoring-metrics'
  AND to_timestamp((node -> 'measured_at')::float)::timestamp > '2022-02-01'::timestamp
  AND to_timestamp((node -> 'measured_at')::float)::timestamp < '2023-04-10'::timestamp
GROUP BY node ->> 'node_id'
