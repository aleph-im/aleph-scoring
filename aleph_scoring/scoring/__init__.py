import asyncio
import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterable, Dict, List, Optional, Set

import asyncpg

from aleph_scoring.config import settings
from aleph_scoring.issue_codes import IssueCode
from aleph_scoring.metrics import get_aleph_nodes
from aleph_scoring.scoring.models import (
    CcnMeasurements,
    CcnScore,
    CrnMeasurements,
    CrnScore,
    NodeScores,
    Score,
)
from aleph_scoring.utils import Period, database_connection

logger = logging.getLogger(__name__)


def read_sql_file(filename: str):
    with open(Path(__file__).parent / "sql" / filename) as fd:
        return fd.read()


async def query_crn_asn_info(
    conn: asyncpg.connection, period: Period
) -> Dict[str, Dict]:
    """Query node autonomous system numbers (ASN).

    ASN is used to compute how decentralized a node is relative to other nodes
    in the network.

    ASN metrics is queried independently of the other metrics
    as to avoid issues related to the group by node_id.
    """
    sql = read_sql_file("query_node_asn_info.template.sql")

    allowed_sender = settings.ALLOWED_METRICS_SENDER

    values = await conn.fetch(
        sql,
        allowed_sender,
        period.from_date.replace(tzinfo=None),
        period.to_date.replace(tzinfo=None),
        "crn",
        settings.ALEPH_POST_TYPE_METRICS,
    )

    result: Dict[str, Dict] = {}
    for row in values:
        if row["node_id"] in result and result[row["node_id"]]["asn"]:
            # Do not update results that contain an ASN,
            # focus on those where it is missing
            continue

        result[row["node_id"]] = {
            "asn": row["asn"],
            "total_nodes": row["total_nodes"],
            "nodes_with_identical_asn": row["nodes_with_identical_asn"],
        }
    return result


async def query_crn_ip_stability(
    conn: asyncpg.connection, period: Period
) -> Dict[str, Dict]:
    """Query per-CRN IPv4/IPv6 presence and change counts over the window.

    The detection window (IP_STABILITY_WINDOW) is independent of the scoring
    period and ends at the period's upper bound.
    """
    sql = read_sql_file("query_crn_ip_stability.sql")

    window_start = period.to_date - settings.IP_STABILITY_WINDOW
    values = await conn.fetch(
        sql,
        settings.ALLOWED_METRICS_SENDER,
        settings.ALEPH_POST_TYPE_METRICS,
        window_start.replace(tzinfo=None),
        period.to_date.replace(tzinfo=None),
    )

    return {
        row["node_id"]: {
            "has_ipv4": row["has_ipv4"],
            "has_ipv6": row["has_ipv6"],
            "ipv4_changes": row["ipv4_changes"],
            "ipv6_changes": row["ipv6_changes"],
            "ipv4": row["current_ipv4"],
            "ipv6_pool": row["current_ipv6_pool"],
            "verified": row["verified"],
        }
        for row in values
    }


async def query_crn_liveness(
    conn: asyncpg.connection, period: Period
) -> Dict[str, Dict]:
    """Query per-CRN proof-of-liveness over the dead-node window.

    Returns ``{node_id: {has_recent_proof, latest_proof}}`` for nodes measured
    in the last DEAD_NODE_WINDOW.
    """
    sql = read_sql_file("query_crn_liveness.sql")

    window_start = period.to_date - settings.DEAD_NODE_WINDOW
    values = await conn.fetch(
        sql,
        settings.ALLOWED_METRICS_SENDER,
        settings.ALEPH_POST_TYPE_METRICS,
        window_start.replace(tzinfo=None),
        period.to_date.replace(tzinfo=None),
    )

    return {
        row["node_id"]: {
            "has_recent_proof": row["has_recent_proof"],
            "latest_proof": row["latest_proof"],
        }
        for row in values
    }


def crn_status(liveness: Optional[Dict]) -> str:
    """Derive a CRN's liveness status from its proof history.

    No proof in the dead-node window (or no entry at all) -> dead. Otherwise
    active if the most recent measurement still proves CRN-ness, else inactive.
    """
    liveness = liveness or {}
    if not liveness.get("has_recent_proof"):
        return "dead"
    return "active" if liveness.get("latest_proof") else "inactive"


def crn_registration_times(node_data: Dict[str, Any]) -> Dict[str, float]:
    """Map each CRN node_id (hash) to its registration timestamp.

    Accepts numeric or numeric-string ``time`` values. Nodes whose time cannot
    be parsed are omitted, which makes them sort last when choosing the keeper
    of a duplicate group (so they never displace a node with a known time).
    """
    times: Dict[str, float] = {}
    for node in node_data.get("resource_nodes", []):
        try:
            times[node["hash"]] = float(node["time"])
        except (KeyError, TypeError, ValueError):
            continue
    return times


def compute_duplicate_crns(
    ip_stability: Dict[str, Dict],
    registration_times: Dict[str, float],
) -> Set[str]:
    """Return node_ids that share an IPv4 or IPv6 VM pool with another CRN.

    For each address (IPv4 and IPv6 pool grouped independently) one node keeps
    its score and the rest are flagged. The keeper is the node that proved its
    identity at /status/config (``verified``) — this defeats DNS spoofing, since
    a node pointing its domain at someone else's server returns the wrong hash
    and is never verified. When no node in the group is verified (e.g. the
    endpoint is unavailable), fall back to the earliest-registered node. Ties
    and unknown registration times are broken by node_id for determinism.
    """
    penalized: Set[str] = set()
    for key in ("ipv4", "ipv6_pool"):
        groups: Dict[str, List[str]] = {}
        for node_id, info in ip_stability.items():
            address = info.get(key)
            if address:
                groups.setdefault(address, []).append(node_id)
        for members in groups.values():
            if len(members) < 2:
                continue
            verified = [n for n in members if ip_stability[n].get("verified")]
            candidates = verified or members
            keeper = min(
                candidates,
                key=lambda n: (registration_times.get(n, float("inf")), n),
            )
            penalized.update(n for n in members if n != keeper)
    return penalized


def _ip_stability_fields(stability: Optional[Dict]) -> Dict:
    """Derive the CrnMeasurements IP fields, including the penalty flag.

    Missing stability data defaults to non-penalizing so nodes are never zeroed
    before enough IP history has accumulated.
    """
    stability = stability or {}
    has_ipv4 = stability.get("has_ipv4", True)
    has_ipv6 = stability.get("has_ipv6", True)
    ipv4_changes = stability.get("ipv4_changes", 0)
    ipv6_changes = stability.get("ipv6_changes", 0)
    ip_penalized = (
        not has_ipv4
        or not has_ipv6
        or ipv4_changes >= settings.IP_MAX_CHANGES
        or ipv6_changes >= settings.IP_MAX_CHANGES
    )
    return {
        "has_ipv4": has_ipv4,
        "has_ipv6": has_ipv6,
        "ipv4_changes": ipv4_changes,
        "ipv6_changes": ipv6_changes,
        "ip_penalized": ip_penalized,
    }


def crn_score_codes(measurements: CrnMeasurements) -> List[IssueCode]:
    """Derive scoring-reason codes from a CRN's aggregated measurements.

    Emitted whenever the condition holds, independently of
    IP_STABILITY_ENFORCED, so operators get early warning before scores drop.
    """
    codes: List[IssueCode] = []
    if not measurements.has_ipv4:
        codes.append(IssueCode.NO_IPV4)
    if not measurements.has_ipv6:
        codes.append(IssueCode.NO_IPV6)
    if measurements.ipv4_changes >= settings.IP_MAX_CHANGES:
        codes.append(IssueCode.IPV4_UNSTABLE)
    if measurements.ipv6_changes >= settings.IP_MAX_CHANGES:
        codes.append(IssueCode.IPV6_UNSTABLE)
    if measurements.duplicate_ip:
        codes.append(IssueCode.DUPLICATE_IP)
    if measurements.status == "dead":
        codes.append(IssueCode.NODE_DEAD)
    elif measurements.status == "inactive":
        codes.append(IssueCode.NODE_INACTIVE)
    return codes


async def query_crn_measurements(
    conn: asyncpg.connection,
    asn_info: Dict,
    ip_stability: Dict,
    duplicate_ids: Set[str],
    liveness: Dict,
    period: Period,
) -> AsyncIterable[tuple[str, CrnMeasurements]]:
    sql = read_sql_file("dev/neo/query_crn_scores.template.sql")

    p1 = 0.99
    p2 = 0.999
    p1_ratio = 0.8
    p2_ratio = 1 - p1_ratio
    values = await conn.fetch(
        sql,
        period.to_date.replace(tzinfo=None),  # $1
        period.from_date.replace(tzinfo=None),  # $2
        p1,  # $3
        p2,  # $4
        settings.ALLOWED_METRICS_SENDER,  # $5
        settings.ALEPH_POST_TYPE_METRICS,  # $6
        p1_ratio,  # $7
        p2_ratio,  # $8
    )

    for record in values:
        node_id = record["node_id"]
        assert isinstance(node_id, str)
        node_asn_info = asn_info.get(node_id)
        if node_asn_info is None:
            logger.warning("No ASN info for CRN node %s, skipping", node_id)
            continue
        row = dict(record)
        row.update(node_asn_info)
        row.update(_ip_stability_fields(ip_stability.get(node_id)))
        row["duplicate_ip"] = node_id in duplicate_ids
        row["status"] = crn_status(liveness.get(node_id))
        yield node_id, CrnMeasurements.parse_obj(row)


async def compute_crn_scores(
    period: Period,
) -> List[CrnScore]:
    conn = await database_connection(settings)

    asn_info: Dict[str, Dict] = await query_crn_asn_info(conn, period=period)
    ip_stability: Dict[str, Dict] = await query_crn_ip_stability(conn, period=period)
    liveness: Dict[str, Dict] = await query_crn_liveness(conn, period=period)

    try:
        node_data = await get_aleph_nodes()
    except Exception:
        logger.exception(
            "Could not fetch node data; skipping duplicate-IP detection this run"
        )
        node_data = {}
    duplicate_ids = compute_duplicate_crns(
        ip_stability, crn_registration_times(node_data)
    )

    # Duplicates are excluded from the per-ASN "identical" count so a Sybil
    # cluster cannot make its ASN look crowded and drag down honest nodes'
    # decentralization. total_nodes is intentionally left untouched.
    duplicate_asn_counts: Counter[int] = Counter()
    if settings.DUPLICATE_IP_ENFORCED:
        duplicate_asn_counts = Counter(
            asn_info[nid]["asn"]
            for nid in duplicate_ids
            if nid in asn_info and asn_info[nid].get("asn") is not None
        )

    result = []
    async for node_id, measurements in query_crn_measurements(
        conn,
        asn_info,
        ip_stability,
        duplicate_ids,
        liveness,
        period,
    ):
        # # This contains custom logic on the scores
        # performance_score = Score(
        #     measurements.base_latency_score_p25
        #     * measurements.base_latency_score_p95
        #     * measurements.diagnostic_vm_latency_score_p25
        #     # Suspend using diagnostic_vm_latency_score_p95 since most nodes
        #     # have very bad values
        #     # * measurements.diagnostic_vm_latency_score_p95
        #     * measurements.full_check_latency_score_p25
        #     # Suspend using full_check_latency_score_p95 since most nodes
        #     # have very bad values
        #     # * measurements.full_check_latency_score_p95
        # ) ** (1 / 4)
        #
        # # This contains custom logic on the scores
        # performance_score = Score(
        #     measurements.base_latency_score_p25
        #     * measurements.base_latency_score_p95
        #     * measurements.diagnostic_vm_latency_score_p25
        #     # Suspend using diagnostic_vm_latency_score_p95 since most nodes
        #     # have very bad values
        #     # * measurements.diagnostic_vm_latency_score_p95
        #     * measurements.full_check_latency_score_p25
        #     # Suspend using full_check_latency_score_p95 since most nodes
        #     # have very bad values
        #     # * measurements.full_check_latency_score_p95
        # ) ** (1 / 4)
        #
        # version_score: Score
        # if not sum(
        #     (
        #         measurements.node_version_missing,
        #         measurements.node_version_latest,
        #         measurements.node_version_outdated,
        #         measurements.node_version_obsolete,
        #         measurements.node_version_other,
        #         measurements.node_version_prerelease,
        #     )
        # ):
        #     logger.warning(f"No version measurement for node {node_id}")
        #     version_score = Score(0)
        # elif (
        #     measurements.node_version_missing
        #     > (
        #         measurements.node_version_latest
        #         + measurements.node_version_outdated
        #         + measurements.node_version_obsolete
        #         + measurements.node_version_other
        #         + measurements.node_version_prerelease
        #     )
        #     / 5
        # ):
        #     # Too many missing version metrics.
        #     version_score = Score(0)
        # else:
        #     version_score = Score(
        #         (
        #             measurements.node_version_latest
        #             + measurements.node_version_outdated
        #             + measurements.node_version_prerelease
        #         )
        #         / (
        #             measurements.node_version_latest
        #             + measurements.node_version_outdated
        #             + measurements.node_version_obsolete
        #             + measurements.node_version_missing
        #             + measurements.node_version_other
        #             + measurements.node_version_prerelease
        #         )
        #     )

        total_score = Score(measurements.total_score)

        if settings.IP_STABILITY_ENFORCED and measurements.ip_penalized:
            logger.info(
                "Zeroing CRN %s for IP instability "
                "(has_ipv4=%s, has_ipv6=%s, ipv4_changes=%d, ipv6_changes=%d)",
                node_id,
                measurements.has_ipv4,
                measurements.has_ipv6,
                measurements.ipv4_changes,
                measurements.ipv6_changes,
            )
            total_score = Score(0)

        if settings.DUPLICATE_IP_ENFORCED and measurements.duplicate_ip:
            logger.info("Zeroing CRN %s as a duplicate-IP node", node_id)
            total_score = Score(0)

        is_dead = settings.DEAD_NODE_ENFORCED and measurements.status == "dead"
        if is_dead:
            logger.info("Zeroing CRN %s: dead (no proof of being a CRN)", node_id)
            total_score = Score(0)

        if is_dead or (settings.DUPLICATE_IP_ENFORCED and measurements.duplicate_ip):
            decentralization_score = Score(0)
        else:
            node_asn = asn_info.get(node_id, {}).get("asn")
            identical = measurements.nodes_with_identical_asn
            if node_asn is not None:
                identical -= duplicate_asn_counts[node_asn]
            decentralization_score = Score(
                (1 - (identical / measurements.total_nodes)) ** 2
            )

        # total_score = Score((performance_score * version_score) ** (1 / 2))

        result.append(
            CrnScore(
                node_id=node_id,
                total_score=total_score,
                decentralization=decentralization_score,
                measurements=measurements,
                status=measurements.status,
                codes=[int(c) for c in crn_score_codes(measurements)],
            )
        )

    await conn.close()

    logger.info(
        "{} CRN nodes with a total score greater than zero".format(
            len([x for x in result if x.total_score > 0])
        )
    )
    return result


async def query_ccn_asn_info(
    conn: asyncpg.connection, period: Period
) -> Dict[str, Dict]:
    """ASN metrics is queried independently of the other metrics
    as to avoid issues related to the group by node_id.
    """
    sql = read_sql_file("query_node_asn_info.template.sql")

    allowed_sender = settings.ALLOWED_METRICS_SENDER
    post_type = settings.ALEPH_POST_TYPE_METRICS

    values = await conn.fetch(
        sql,
        allowed_sender,
        period.from_date.replace(tzinfo=None),
        period.to_date.replace(tzinfo=None),
        "ccn",
        post_type,
    )

    result: Dict[str, Dict] = {}
    for row in values:
        if row["node_id"] in result and result[row["node_id"]]["asn"]:
            # Do not update results that contain an ASN,
            # focus on those where it is missing
            continue

        result[row["node_id"]] = {
            "asn": row["asn"],
            "total_nodes": row["total_nodes"],
            "nodes_with_identical_asn": row["nodes_with_identical_asn"],
        }
    return result


async def query_ccn_measurements(
    conn: asyncpg.connection,
    asn_info: Dict,
    period: Period,
) -> AsyncIterable[tuple[str, CcnMeasurements]]:
    sql = read_sql_file("dev/neo/query_ccn_scores.template.sql")

    p1 = 0.99
    p2 = 0.999
    p1_ratio = 0.8
    p2_ratio = 1 - p1_ratio
    values = await conn.fetch(
        sql,
        period.to_date.replace(tzinfo=None),  # $1
        period.from_date.replace(tzinfo=None),  # $2
        p1,  # $3
        p2,  # $4
        settings.ALLOWED_METRICS_SENDER,  # $5
        settings.ALEPH_POST_TYPE_METRICS,  # $6
        p1_ratio,  # $7
        p2_ratio,  # $8
    )

    for record in values:
        node_id = record["node_id"]
        node_asn_info = asn_info.get(node_id)
        if node_asn_info is None:
            logger.warning("No ASN info for CCN node %s, skipping", node_id)
            continue
        row = dict(record)
        row.update(node_asn_info)
        yield node_id, CcnMeasurements.parse_obj(row)


async def compute_ccn_scores(
    period: Period,
) -> List[CcnScore]:
    conn = await database_connection(settings)

    asn_info: Dict[str, Dict] = await query_ccn_asn_info(conn, period=period)

    result = []
    measurements: CcnMeasurements
    async for node_id, measurements in query_ccn_measurements(
        conn,
        asn_info,
        period,
    ):
        # # This contains custom logic on the scores
        # performance_score = (
        #     measurements.base_latency_score_p25
        #     * measurements.base_latency_score_p95
        #     * measurements.metrics_latency_score_p25
        #     # Suspend using diagnostic_vm_latency_score_p95 since most nodes
        #     # have very bad values
        #     * measurements.metrics_latency_score_p95
        #     * measurements.aggregate_latency_score_p25
        #     # Suspend using full_check_latency_score_p95 since most nodes
        #     # have very bad values
        #     * measurements.aggregate_latency_score_p95
        #     # * measurements.file_download_latency_score_p25
        #     # * measurements.file_download_latency_score_p95
        #     # * measurements.eth_height_remaining_score_p25
        #     # * measurements.eth_height_remaining_score_p95
        # ) ** (1 / 6)
        #
        # if not sum(
        #     (
        #         measurements.node_version_missing,
        #         measurements.node_version_latest,
        #         measurements.node_version_outdated,
        #         measurements.node_version_obsolete,
        #         measurements.node_version_other,
        #         measurements.node_version_prerelease,
        #     )
        # ):
        #     logger.warning(f"No version measurement for node {node_id}")
        #     version_score = Score(0)
        # elif (
        #     measurements.node_version_missing
        #     > (
        #         measurements.node_version_latest
        #         + measurements.node_version_outdated
        #         + measurements.node_version_obsolete
        #         + measurements.node_version_other
        #         + measurements.node_version_prerelease
        #     )
        #     / 5
        # ):
        #     logger.debug(f"Too many missing version metrics for CRN node {node_id}")
        #     version_score = Score(0)
        # else:
        #     version_score = (
        #         measurements.node_version_latest
        #         + measurements.node_version_outdated
        #         + measurements.node_version_prerelease
        #     ) / (
        #         measurements.node_version_latest
        #         + measurements.node_version_outdated
        #         + measurements.node_version_obsolete
        #         + measurements.node_version_missing
        #         + measurements.node_version_other
        #         + measurements.node_version_prerelease
        #     )

        total_score = Score(measurements.total_score)

        decentralization_score = Score(
            (1 - (measurements.nodes_with_identical_asn / measurements.total_nodes))
            ** 2
        )

        # total_score = (performance_score * version_score) ** (1 / 2)

        result.append(
            CcnScore(
                node_id=node_id,
                total_score=total_score,
                decentralization=decentralization_score,
                measurements=measurements,
            )
        )

    await conn.close()

    logger.info(
        "{} CCN nodes with a total score greater than zero".format(
            len([x for x in result if x.total_score > 0])
        )
    )
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    to_date = datetime.now(tz=timezone.utc)
    from_date = to_date - settings.SCORE_METRICS_PERIOD
    current_period = Period(from_date=from_date, to_date=to_date)

    ccn_scores = asyncio.run(
        compute_ccn_scores(
            period=current_period,
        )
    )
    crn_scores = asyncio.run(
        compute_crn_scores(
            period=current_period,
        )
    )

    scores = NodeScores(
        ccn=ccn_scores,
        crn=crn_scores,
    )
    with open(Path(__file__).parent.parent.parent / "scores.json", "w") as fd:
        fd.write(scores.json(indent=4))
