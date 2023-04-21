SELECT node ->> 'node_id'                                                        as node_id,

       count((node -> 'base_latency')::float > 0)                                as base_latency_present,
       count(case when (node -> 'base_latency')::float is null then 1 end)       as base_latency_missing,

    /* Compute the score as 1 - half of the base latency. The worst pings across Earth are around 600ms, so any
       decent server should be able to respond within 2 seconds and have a score. */
       greatest(
                   1 - percentile_disc(0.25) WITHIN GROUP (ORDER BY COALESCE((node -> 'base_latency')::float, 100.)) / 2,
                   0
           )                                                                    as base_latency_score_p25,

    /* Compute the score as 1 - half of the base latency. The worst pings across Earth are around 600ms, so any
       decent server should be able to respond within 2 seconds and have a score. */
       greatest(
                   1 - percentile_disc(0.95) WITHIN GROUP (ORDER BY COALESCE((node -> 'base_latency')::float, 100.)) / 2,
                   0
           )                                                                    as base_latency_score_p95,

       greatest(
                   1 - percentile_disc(0.25)
                       WITHIN GROUP (ORDER BY COALESCE((node -> 'diagnostic_vm_latency')::float, 100.)) / 2.5,
                   0
           )                                                                    as diagnostic_vm_latency_score_p25,

       greatest(
                   1 - percentile_disc(0.95)
                       WITHIN GROUP (ORDER BY COALESCE((node -> 'diagnostic_vm_latency')::float, 100.)) / 2.5,
                   0
           )                                                                    as diagnostic_vm_latency_score_p95,

       greatest(
                   1 -
                   percentile_disc(0.25) WITHIN GROUP (ORDER BY COALESCE((node -> 'full_check_latency')::float, 100.)) /
                   4,
                   0
           )                                                                    as full_check_latency_score_p25,

       greatest(
                   1 -
                   percentile_disc(0.95) WITHIN GROUP (ORDER BY COALESCE((node -> 'full_check_latency')::float, 100.)) /
                   4,
                   0
           )                                                                    as full_check_latency_score_p95,

       count((node -> 'full_check_latency')::float > 0)                          as full_check_latency_present,
       count(case when (node -> 'full_check_latency')::float is null then 1 end) as full_check_latency_missing,

       count(
            case
                when (
                    annotate_version('aleph-vm', node ->> 'version',
                                     to_timestamp((node ->> 'measured_at')::float)::date) = 'latest'
                    ) then 1 end)
            as node_version_latest,

        count(
            case
                when (
                    annotate_version('aleph-vm', node ->> 'version',
                                     to_timestamp((node ->> 'measured_at')::float)::date) = 'prerelease'
                    ) then 1 end)
            as node_version_prerelease,

        count(
            case
                when (
                    annotate_version('aleph-vm', node ->> 'version',
                                     to_timestamp((node ->> 'measured_at')::float)::date) = 'outdated'
                    ) then 1 end)
            as node_version_outdated,

        count(
            case
                when (
                    annotate_version('aleph-vm', node ->> 'version',
                                     to_timestamp((node ->> 'measured_at')::float)::date) = 'obsolete'
                    ) then 1 end)
            as node_version_obsolete,

        count(
            case
                when (
                    annotate_version('aleph-vm', node ->> 'version',
                                     to_timestamp((node ->> 'measured_at')::float)::date) = 'other'
                    ) then 1 end)
            as node_version_other,

        count(case when (coalesce(node ->> 'version', '') = '') then 1 end) as node_version_missing
FROM posts,
     jsonb_array_elements(content -> 'metrics' -> 'crn') node
WHERE owner = $1
    /*owner = ANY($3::text[])*/
  AND type = $2
  AND to_timestamp((node -> 'measured_at')::float)::timestamp > $3::timestamp
  AND to_timestamp((node -> 'measured_at')::float)::timestamp < $4::timestamp
GROUP BY node ->> 'node_id'
