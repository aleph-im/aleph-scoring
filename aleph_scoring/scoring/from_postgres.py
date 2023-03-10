import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import asyncpg

from aleph_scoring.config import settings
from aleph_scoring.scoring.models import (
    CcnMeasurements,
    CcnScore,
    CrnMeasurements,
    CrnScore,
    NodeScores,
)
from aleph_scoring.utils import (
    Period,
    database_connection,
    GithubRelease,
    get_latest_github_releases,
)

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
        period.from_date,
        period.to_date,
        "crn",
    )

    result = {}
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


async def query_crn_measurements(
    conn: asyncpg.connection,
    asn_info: Dict,
    period: Period,
    last_release: GithubRelease,
    previous_release: GithubRelease,
):
    sql = read_sql_file("query_crn_measurements.template.sql")

    select_last_version = last_release.tag_name
    select_previous_version = previous_release.tag_name
    release_date = last_release.published_at
    select_update_deadline = release_date + settings.VERSION_GRACE_PERIOD
    select_trusted_owner = settings.ALLOWED_METRICS_SENDER

    values = await conn.fetch(
        sql,
        select_last_version,
        select_update_deadline,
        select_trusted_owner,
        period.from_date,
        period.to_date,
        select_previous_version,
    )

    for record in values:
        row = dict(record)
        row.update(asn_info[record["node_id"]])
        yield record["node_id"], CrnMeasurements.parse_obj(row)


async def compute_crn_scores(
    period: Period, last_release: GithubRelease, previous_release
) -> List[CrnScore]:
    conn = await database_connection(settings)

    asn_info: Dict[str, Dict] = await query_crn_asn_info(conn, period=period)

    result = []
    async for node_id, measurements in query_crn_measurements(
        conn, asn_info, period, last_release, previous_release
    ):
        # This contains custom logic on the scores
        performance_score = (
            measurements.base_latency_score_p25
            * measurements.base_latency_score_p95
            * measurements.diagnostic_vm_latency_score_p25
            # Suspend using diagnostic_vm_latency_score_p95 since most nodes
            # have very bad values
            # * measurements.diagnostic_vm_latency_score_p95
            * measurements.full_check_latency_score_p25
            # Suspend using full_check_latency_score_p95 since most nodes
            # have very bad values
            # * measurements.full_check_latency_score_p95
        ) ** (1 / 4)

        if (
            measurements.node_version_missing
            > (
                measurements.node_version_latest
                + measurements.node_version_outdated
                + measurements.node_version_obsolete
            )
            / 5
        ):
            # Too many missing version metrics.
            version_score = 0
        else:
            version_score = (
                measurements.node_version_latest + measurements.node_version_outdated
            ) / (
                measurements.node_version_latest
                + measurements.node_version_outdated
                + measurements.node_version_obsolete
                + measurements.node_version_missing
            )

        decentralization_score = 1 - (
            measurements.nodes_with_identical_asn / measurements.total_nodes
        )

        total_score = (performance_score * version_score * decentralization_score) ** (
            1 / 3
        )

        result.append(
            CrnScore(
                node_id=node_id,
                total_score=total_score,
                performance=performance_score,
                version=version_score,
                decentralization=decentralization_score,
                measurements=measurements,
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

    values = await conn.fetch(
        sql,
        allowed_sender,
        period.from_date,
        period.to_date,
        "ccn",
    )

    result = {}
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
    last_release: GithubRelease,
    previous_release: GithubRelease,
):
    sql = read_sql_file("query_ccn_measurements.template.sql")

    select_last_version = last_release.tag_name
    select_previous_version = previous_release.tag_name
    release_date = last_release.published_at
    select_update_deadline = release_date + settings.VERSION_GRACE_PERIOD
    select_trusted_owner = settings.ALLOWED_METRICS_SENDER

    values = await conn.fetch(
        sql,
        select_last_version,
        select_update_deadline,
        select_trusted_owner,
        period.from_date,
        period.to_date,
        select_previous_version,
    )

    for record in values:
        row = dict(record)
        row.update(asn_info[record["node_id"]])
        yield record["node_id"], CcnMeasurements.parse_obj(row)


async def compute_ccn_scores(
    period: Period, last_release, previous_release
) -> List[CcnScore]:
    conn = await database_connection(settings)

    asn_info: Dict[str, Dict] = await query_ccn_asn_info(conn, period=period)

    result = []
    async for node_id, measurements in query_ccn_measurements(
        conn, asn_info, period, last_release, previous_release
    ):
        # This contains custom logic on the scores
        performance_score = (
            measurements.base_latency_score_p25
            * measurements.base_latency_score_p95
            * measurements.metrics_latency_score_p25
            # Suspend using diagnostic_vm_latency_score_p95 since most nodes
            # have very bad values
            * measurements.metrics_latency_score_p95
            * measurements.aggregate_latency_score_p25
            # Suspend using full_check_latency_score_p95 since most nodes
            # have very bad values
            * measurements.aggregate_latency_score_p95
            * measurements.file_download_latency_score_p25
            * measurements.file_download_latency_score_p95
            # * measurements.eth_height_remaining_score_p25
            # * measurements.eth_height_remaining_score_p95
        ) ** (1 / 8)

        if (
            measurements.node_version_missing
            > (
                measurements.node_version_latest
                + measurements.node_version_outdated
                + measurements.node_version_obsolete
            )
            / 5
        ):
            # Too many missing version metrics.
            version_score = 0
        else:
            version_score = (
                measurements.node_version_latest + measurements.node_version_outdated
            ) / (
                measurements.node_version_latest
                + measurements.node_version_outdated
                + measurements.node_version_obsolete
                + measurements.node_version_missing
            )

        decentralization_score = 1 - (
            measurements.nodes_with_identical_asn / measurements.total_nodes
        )

        total_score = (performance_score * version_score * decentralization_score) ** (
            1 / 3
        )

        result.append(
            CcnScore(
                node_id=node_id,
                total_score=total_score,
                performance=performance_score,
                version=version_score,
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

    to_date = datetime.utcnow()
    from_date = to_date - settings.SCORE_METRICS_PERIOD

    latest_ccn_release, previous_ccn_release = get_latest_github_releases(
        "aleph-im", "pyaleph"
    )
    latest_crn_release, previous_crn_release = get_latest_github_releases(
        "aleph-im", "aleph-vm"
    )

    current_period = Period(from_date, to_date)
    ccn_scores = asyncio.run(
        compute_ccn_scores(
            period=current_period,
            last_release=latest_ccn_release,
            previous_release=previous_ccn_release,
        )
    )
    crn_scores = asyncio.run(
        compute_crn_scores(
            period=current_period,
            last_release=latest_crn_release,
            previous_release=previous_crn_release,
        )
    )

    scores = NodeScores(
        ccn=ccn_scores,
        crn=crn_scores,
    )
    with open(Path(__file__).parent.parent.parent / "scores.json", "w") as fd:
        fd.write(scores.json(indent=4))
