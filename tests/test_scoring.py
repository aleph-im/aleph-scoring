from aleph_scoring.schemas.metrics import CrnMetrics, CcnMetrics
import datetime as dt

from aleph_scoring.scoring import compute_crn_scores, compute_ccn_scores


def test_scoring_ccn():
    ccn_metrics = [CcnMetrics(
        measured_at=dt.datetime(2022, 12, 26),
        node_id="1",
        url="https://api1.aleph.im",
        asn=1,
        as_name="Aleph.im",
        base_latency=0.4,
        metrics_latency=0.3,
        aggregate_latency=0.7,
        file_download_latency=0.4,
        pending_messages=204,
        eth_height_remaining=0,
    ),
        CcnMetrics(
            measured_at=dt.datetime(2022, 12, 26),
            node_id="2",
            url="https://api2.aleph.im",
            asn=2,
            as_name="Amazon Web Services",
            base_latency=0.5,
            metrics_latency=0.5,
            aggregate_latency=0.5,
            file_download_latency=0.5,
            pending_messages=0,
            eth_height_remaining=3,
        )
    ]

    ccn_scores = compute_ccn_scores(ccn_metrics)
    assert len(ccn_scores) == 2
    assert ccn_scores[0].decentralization == 0.5


def test_scoring_crn():
    crn_metrics = CrnMetrics(
        measured_at=dt.datetime(2022, 12, 26),
        node_id="1234",
        url="https://aleph.sh",
        asn=1,
        as_name="Aleph.im",
        base_latency=0.3,
        diagnostic_vm_latency=0.7,
        full_check_latency=0.7,
    )

    crn_scores = compute_crn_scores([crn_metrics])
    assert len(crn_scores) == 1
    crn_score = crn_scores[0]

    assert crn_score.decentralization == 0
