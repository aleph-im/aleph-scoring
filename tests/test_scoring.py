import datetime as dt

import pytest

from aleph_scoring import scoring
from aleph_scoring.issue_codes import IssueCode
from aleph_scoring.scoring import (
    compute_ccn_scores,
    compute_crn_scores,
    compute_duplicate_crns,
    crn_score_codes,
    crn_status,
)
from aleph_scoring.scoring.models import CcnMeasurements, CrnMeasurements
from aleph_scoring.utils import Period


class _FakeConnection:
    """Stand-in for the asyncpg connection opened by compute_*_scores."""

    async def close(self) -> None:
        pass


@pytest.fixture
def period() -> Period:
    return Period(
        from_date=dt.datetime(2022, 12, 1, tzinfo=dt.timezone.utc),
        to_date=dt.datetime(2022, 12, 26, tzinfo=dt.timezone.utc),
    )


def _patch_db(
    monkeypatch, *, asn_info_name, measurements_name, rows, stability_name=None
):
    """Replace the DB boundary so the scoring functions run on `rows`."""

    async def fake_connection(_settings):
        return _FakeConnection()

    async def fake_asn_info(_conn, period):
        return {}

    async def fake_measurements(*_args):
        for node_id, measurements in rows:
            yield node_id, measurements

    monkeypatch.setattr(scoring, "database_connection", fake_connection)
    monkeypatch.setattr(scoring, asn_info_name, fake_asn_info)
    monkeypatch.setattr(scoring, measurements_name, fake_measurements)

    if stability_name:

        async def fake_stability(_conn, period):
            return {}

        async def fake_nodes():
            return {"resource_nodes": []}

        async def fake_liveness(_conn, period):
            # Every node proves liveness, so status defaults to active in tests
            # that don't care about it.
            return {
                nid: {"has_recent_proof": True, "latest_proof": True} for nid, _ in rows
            }

        monkeypatch.setattr(scoring, stability_name, fake_stability)
        monkeypatch.setattr(scoring, "get_aleph_nodes", fake_nodes)
        monkeypatch.setattr(scoring, "query_crn_liveness", fake_liveness)


@pytest.mark.asyncio
async def test_compute_ccn_scores(monkeypatch, period):
    rows = [
        (
            "node-1",
            CcnMeasurements(
                total_nodes=2,
                nodes_with_identical_asn=1,
                record_count=10,
                total_score=0.8,
            ),
        ),
        (
            "node-2",
            CcnMeasurements(
                total_nodes=2,
                nodes_with_identical_asn=2,
                record_count=10,
                total_score=0.5,
            ),
        ),
    ]
    _patch_db(
        monkeypatch,
        asn_info_name="query_ccn_asn_info",
        measurements_name="query_ccn_measurements",
        rows=rows,
    )

    scores = await compute_ccn_scores(period=period)

    assert [s.node_id for s in scores] == ["node-1", "node-2"]
    assert scores[0].total_score == 0.8
    # Alone in its ASN: (1 - 1/2) ** 2
    assert scores[0].decentralization == 0.25
    # Shares its ASN with every node: (1 - 2/2) ** 2
    assert scores[1].decentralization == 0


@pytest.mark.asyncio
async def test_compute_crn_scores(monkeypatch, period):
    rows = [
        (
            "crn-1",
            CrnMeasurements(
                total_nodes=4,
                nodes_with_identical_asn=1,
                record_count=5,
                total_score=0.9,
            ),
        ),
    ]
    _patch_db(
        monkeypatch,
        asn_info_name="query_crn_asn_info",
        measurements_name="query_crn_measurements",
        stability_name="query_crn_ip_stability",
        rows=rows,
    )

    scores = await compute_crn_scores(period=period)

    assert len(scores) == 1
    assert scores[0].node_id == "crn-1"
    assert scores[0].total_score == 0.9
    # (1 - 1/4) ** 2
    assert scores[0].decentralization == 0.5625


@pytest.mark.parametrize(
    "stability, expected",
    [
        (None, False),  # no history yet -> never penalized
        (
            {"has_ipv4": True, "has_ipv6": True, "ipv4_changes": 1, "ipv6_changes": 0},
            False,
        ),
        (
            {"has_ipv4": False, "has_ipv6": True, "ipv4_changes": 0, "ipv6_changes": 0},
            True,
        ),
        (
            {"has_ipv4": True, "has_ipv6": False, "ipv4_changes": 0, "ipv6_changes": 0},
            True,
        ),
        (
            {"has_ipv4": True, "has_ipv6": True, "ipv4_changes": 2, "ipv6_changes": 0},
            True,
        ),
        (
            {"has_ipv4": True, "has_ipv6": True, "ipv4_changes": 0, "ipv6_changes": 3},
            True,
        ),
    ],
)
def test_ip_stability_penalty_rule(stability, expected):
    assert scoring._ip_stability_fields(stability)["ip_penalized"] is expected


def _crn_measurements(**overrides) -> CrnMeasurements:
    base = dict(
        total_nodes=1, nodes_with_identical_asn=1, record_count=1, total_score=0.9
    )
    base.update(overrides)
    return CrnMeasurements(**base)


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({}, []),  # clean node -> no codes
        ({"has_ipv4": False}, [IssueCode.NO_IPV4]),
        ({"has_ipv6": False}, [IssueCode.NO_IPV6]),
        ({"ipv4_changes": 2}, [IssueCode.IPV4_UNSTABLE]),
        ({"ipv6_changes": 3}, [IssueCode.IPV6_UNSTABLE]),
        (
            {"has_ipv6": False, "ipv4_changes": 5},
            [IssueCode.NO_IPV6, IssueCode.IPV4_UNSTABLE],
        ),
        ({"duplicate_ip": True}, [IssueCode.DUPLICATE_IP]),
    ],
)
def test_crn_score_codes(overrides, expected):
    assert crn_score_codes(_crn_measurements(**overrides)) == expected


@pytest.mark.parametrize(
    "ip_stability, times, expected",
    [
        # Unique addresses -> no duplicates
        (
            {"a": {"ipv4": "1.1.1.1"}, "b": {"ipv4": "2.2.2.2"}},
            {"a": 1, "b": 2},
            set(),
        ),
        # Same IPv4: later registration penalized, earliest kept
        (
            {"a": {"ipv4": "1.1.1.1"}, "b": {"ipv4": "1.1.1.1"}},
            {"a": 1, "b": 2},
            {"b"},
        ),
        # Same /64: earliest kept
        (
            {"a": {"ipv6_prefix": "2a01::/64"}, "b": {"ipv6_prefix": "2a01::/64"}},
            {"a": 5, "b": 3},
            {"a"},
        ),
        # Shares IPv4 with one and /64 with another -> only earliest survives
        (
            {
                "a": {"ipv4": "1.1.1.1", "ipv6_prefix": "2a01::/64"},
                "b": {"ipv4": "1.1.1.1"},
                "c": {"ipv6_prefix": "2a01::/64"},
            },
            {"a": 1, "b": 2, "c": 3},
            {"b", "c"},
        ),
        # Null addresses are not grouped
        (
            {"a": {"ipv4": None, "ipv6_prefix": None}, "b": {"ipv4": None}},
            {"a": 1, "b": 2},
            set(),
        ),
        # Unknown registration time sorts last (never keeper)
        (
            {"a": {"ipv4": "1.1.1.1"}, "b": {"ipv4": "1.1.1.1"}},
            {"b": 2},  # "a" has no time
            {"a"},
        ),
        # Anti-grief: the verified owner is kept even though an older spoofer
        # (pointing its domain at the owner's IP) shares the address.
        (
            {
                "spoofer": {"ipv4": "1.1.1.1"},
                "owner": {"ipv4": "1.1.1.1", "verified": True},
            },
            {"spoofer": 1, "owner": 2},
            {"spoofer"},
        ),
        # Multiple verified -> earliest verified is kept
        (
            {
                "a": {"ipv4": "1.1.1.1", "verified": True},
                "b": {"ipv4": "1.1.1.1", "verified": True},
            },
            {"a": 2, "b": 1},
            {"a"},
        ),
    ],
)
def test_compute_duplicate_crns(ip_stability, times, expected):
    assert compute_duplicate_crns(ip_stability, times) == expected


@pytest.mark.asyncio
async def test_compute_crn_scores_zeroes_penalized_when_enforced(monkeypatch, period):
    monkeypatch.setattr(scoring.settings, "IP_STABILITY_ENFORCED", True)
    rows = [
        (
            "crn-bad",
            CrnMeasurements(
                total_nodes=2,
                nodes_with_identical_asn=1,
                record_count=5,
                total_score=0.95,
                has_ipv4=True,
                has_ipv6=False,
                ip_penalized=True,
            ),
        ),
    ]
    _patch_db(
        monkeypatch,
        asn_info_name="query_crn_asn_info",
        measurements_name="query_crn_measurements",
        stability_name="query_crn_ip_stability",
        rows=rows,
    )

    scores = await compute_crn_scores(period=period)

    assert scores[0].total_score == 0  # zeroed despite total_score=0.95


@pytest.mark.asyncio
async def test_compute_crn_scores_zeroes_duplicate_when_enforced(monkeypatch, period):
    monkeypatch.setattr(scoring.settings, "DUPLICATE_IP_ENFORCED", True)
    rows = [
        (
            "crn-dup",
            CrnMeasurements(
                total_nodes=2,
                nodes_with_identical_asn=1,
                record_count=5,
                total_score=0.95,
                duplicate_ip=True,
            ),
        ),
    ]
    _patch_db(
        monkeypatch,
        asn_info_name="query_crn_asn_info",
        measurements_name="query_crn_measurements",
        stability_name="query_crn_ip_stability",
        rows=rows,
    )

    scores = await compute_crn_scores(period=period)

    assert scores[0].total_score == 0
    assert scores[0].decentralization == 0  # zeroed alongside the score
    assert int(IssueCode.DUPLICATE_IP) in scores[0].codes


@pytest.mark.asyncio
async def test_compute_crn_scores_keeps_duplicate_when_not_enforced(
    monkeypatch, period
):
    monkeypatch.setattr(scoring.settings, "DUPLICATE_IP_ENFORCED", False)
    rows = [
        (
            "crn-dup",
            CrnMeasurements(
                total_nodes=2,
                nodes_with_identical_asn=1,
                record_count=5,
                total_score=0.95,
                duplicate_ip=True,
            ),
        ),
    ]
    _patch_db(
        monkeypatch,
        asn_info_name="query_crn_asn_info",
        measurements_name="query_crn_measurements",
        stability_name="query_crn_ip_stability",
        rows=rows,
    )

    scores = await compute_crn_scores(period=period)

    # Score is NOT zeroed, but the code is still published for early warning.
    assert scores[0].total_score == 0.95
    assert int(IssueCode.DUPLICATE_IP) in scores[0].codes


@pytest.mark.asyncio
async def test_compute_crn_scores_skips_dedup_when_node_fetch_fails(
    monkeypatch, period
):
    monkeypatch.setattr(scoring.settings, "DUPLICATE_IP_ENFORCED", True)
    rows = [
        (
            "crn-1",
            CrnMeasurements(
                total_nodes=2,
                nodes_with_identical_asn=1,
                record_count=5,
                total_score=0.9,
            ),
        ),
    ]
    _patch_db(
        monkeypatch,
        asn_info_name="query_crn_asn_info",
        measurements_name="query_crn_measurements",
        stability_name="query_crn_ip_stability",
        rows=rows,
    )

    async def boom():
        raise RuntimeError("node API unreachable")

    monkeypatch.setattr(scoring, "get_aleph_nodes", boom)

    # Scoring still completes; duplicate detection is simply skipped.
    scores = await compute_crn_scores(period=period)

    assert len(scores) == 1
    assert scores[0].total_score == 0.9
    assert int(IssueCode.DUPLICATE_IP) not in scores[0].codes


@pytest.mark.asyncio
async def test_decentralization_excludes_duplicates_from_asn(monkeypatch, period):
    # ASN 1234 has 4 nodes: 2 duplicates + 2 honest. The duplicates are
    # subtracted from the honest node's identical count: (1 - (4 - 2) / 4) ** 2.
    monkeypatch.setattr(scoring.settings, "DUPLICATE_IP_ENFORCED", True)
    asn_entry = {"asn": 1234, "total_nodes": 4, "nodes_with_identical_asn": 4}
    asn_info = {"honest1": asn_entry, "dup1": asn_entry, "dup2": asn_entry}
    rows = [
        (
            "honest1",
            CrnMeasurements(
                total_nodes=4,
                nodes_with_identical_asn=4,
                record_count=5,
                total_score=0.9,
            ),
        ),
    ]
    _patch_db(
        monkeypatch,
        asn_info_name="query_crn_asn_info",
        measurements_name="query_crn_measurements",
        stability_name="query_crn_ip_stability",
        rows=rows,
    )

    async def fake_asn_info(_conn, period):
        return asn_info

    monkeypatch.setattr(scoring, "query_crn_asn_info", fake_asn_info)
    monkeypatch.setattr(
        scoring, "compute_duplicate_crns", lambda ip, t: {"dup1", "dup2"}
    )

    scores = await compute_crn_scores(period=period)

    assert scores[0].node_id == "honest1"
    assert scores[0].decentralization == 0.25  # (1 - (4 - 2) / 4) ** 2


@pytest.mark.parametrize(
    "liveness, expected",
    [
        (None, "dead"),  # no entry -> never measured recently
        ({"has_recent_proof": False, "latest_proof": False}, "dead"),
        ({"has_recent_proof": True, "latest_proof": True}, "active"),
        ({"has_recent_proof": True, "latest_proof": False}, "inactive"),
    ],
)
def test_crn_status(liveness, expected):
    assert crn_status(liveness) == expected


@pytest.mark.asyncio
async def test_compute_crn_scores_zeroes_dead_when_enforced(monkeypatch, period):
    monkeypatch.setattr(scoring.settings, "DEAD_NODE_ENFORCED", True)
    rows = [
        (
            "crn-dead",
            CrnMeasurements(
                total_nodes=2,
                nodes_with_identical_asn=1,
                record_count=5,
                total_score=0.95,
                status="dead",
            ),
        ),
    ]
    _patch_db(
        monkeypatch,
        asn_info_name="query_crn_asn_info",
        measurements_name="query_crn_measurements",
        stability_name="query_crn_ip_stability",
        rows=rows,
    )

    scores = await compute_crn_scores(period=period)

    assert scores[0].total_score == 0
    assert scores[0].decentralization == 0
    assert scores[0].status == "dead"
    assert int(IssueCode.NODE_DEAD) in scores[0].codes


@pytest.mark.asyncio
async def test_compute_crn_scores_keeps_dead_when_not_enforced(monkeypatch, period):
    monkeypatch.setattr(scoring.settings, "DEAD_NODE_ENFORCED", False)
    rows = [
        (
            "crn-dead",
            CrnMeasurements(
                total_nodes=2,
                nodes_with_identical_asn=1,
                record_count=5,
                total_score=0.95,
                status="dead",
            ),
        ),
    ]
    _patch_db(
        monkeypatch,
        asn_info_name="query_crn_asn_info",
        measurements_name="query_crn_measurements",
        stability_name="query_crn_ip_stability",
        rows=rows,
    )

    scores = await compute_crn_scores(period=period)

    # Status and code are published for schedulers, but the score is untouched.
    assert scores[0].total_score == 0.95
    assert scores[0].status == "dead"
    assert int(IssueCode.NODE_DEAD) in scores[0].codes
