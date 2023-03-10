SELECT
    crn->'node_id' as node_id,
    crn->'url' as node_url,

    /*
    percentile_cont(0.25) WITHIN GROUP (ORDER BY (crn->'base_latency')::float) as base_latency_percentile_25,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY (crn->'base_latency')::float) as base_latency_percentile_50,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY (crn->'base_latency')::float) as base_latency_percentile_75,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY (crn->'base_latency')::float) as base_latency_percentile_95,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY (crn->'base_latency')::float) as base_latency_percentile_99,
    */

    count((crn->'base_latency')::float > 0) as base_latency_present,
    count(case when (crn->'base_latency')::float is null then 1 end) as base_latency_missing,

    /* Compute the score as 1 - half of the base latency. The worst pings across Earth are around 600ms, so any
       decent server should be able to respond within 2 seconds and have a score. */
    greatest(
        1 - percentile_disc(0.25) WITHIN GROUP (ORDER BY COALESCE((crn->'base_latency')::float, 100.)) / 2,
        0
    ) as base_latency_score_p25,

    /* Compute the score as 1 - half of the base latency. The worst pings across Earth are around 600ms, so any
       decent server should be able to respond within 2 seconds and have a score. */
    greatest(
        1 - percentile_disc(0.95) WITHIN GROUP (ORDER BY COALESCE((crn->'base_latency')::float, 100.)) / 2,
        0
    ) as base_latency_score_p95,

    greatest(
        1 - percentile_disc(0.25) WITHIN GROUP (ORDER BY COALESCE((crn->'diagnostic_vm_latency')::float, 100.)) / 2.5,
        0
    ) as diagnostic_vm_latency_score_p25,

    greatest(
        1 - percentile_disc(0.95) WITHIN GROUP (ORDER BY COALESCE((crn->'diagnostic_vm_latency')::float, 100.)) / 2.5,
        0
    ) as diagnostic_vm_latency_score_p95,

    greatest(
        1 - percentile_disc(0.25) WITHIN GROUP (ORDER BY COALESCE((crn->'full_check_latency')::float, 100.)) / 4,
        0
    ) as full_check_latency_score_p25,

    greatest(
        1 - percentile_disc(0.95) WITHIN GROUP (ORDER BY COALESCE((crn->'full_check_latency')::float, 100.)) / 4,
        0
    ) as full_check_latency_score_p95,

    count((crn->'full_check_latency')::float > 0) as full_check_latency_present,
    count(case when (crn->'full_check_latency')::float is null then 1 end) as full_check_latency_missing,

--     count(case when (crn->'version')::text = '0.2.5' then 1 end) as node_version
    count(case when (crn->>'version' = '0.2.5') then 1 end) as node_version_latest,
    count(case when (
        crn->>'version' = '0.2.4' and
        to_timestamp((crn->'measured_at')::float)::date > '2023-01-30'::date
    ) then 1 end) as node_version_previous
FROM
    posts, jsonb_array_elements(content->'metrics'->'crn') crn
WHERE
    owner = '0x4d741d44348B21e97000A8C9f07Ee34110F7916F'
    AND type = 'aleph-scoring-metrics'
    AND to_timestamp((crn->'measured_at')::float)::date > '2023-01-30'::date
GROUP BY
    crn->'node_id',
    crn->'url'
ORDER BY
    base_latency_score_p95 DESC
