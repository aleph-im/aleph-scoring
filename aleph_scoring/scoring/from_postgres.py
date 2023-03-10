import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple

import asyncpg

from aleph_scoring.scoring.models import CrnScore, CrnMeasurements, NodeScores, CcnMeasurements, CcnScore

ALLOWED_METRICS_SENDER = "0x4d741d44348B21e97000A8C9f07Ee34110F7916F"


def read_sql_file(filename: str):
    with open(Path(__file__).parent / "sql" / filename) as fd:
        return fd.read()


async def query_crn_asn_info(conn: asyncpg.connection) -> Dict[str, Dict]:
    """Query node autonomous system numbers (ASN).

    ASN is used to compute how decentralized a node is relative to other nodes
    in the network.

    ASN metrics is queried independently of the other metrics
    as to avoid issues related to the group by node_id.
    """
    sql = read_sql_file("query_node_asn_info.template.sql")

    allowed_sender = ALLOWED_METRICS_SENDER
    date_gt = datetime.fromisoformat("2022-12-01")
    date_lt = datetime.fromisoformat("2023-03-10")

    values = await conn.fetch(
        sql,
        allowed_sender,
        date_gt,
        date_lt,
        'crn',
    )

    result = {}
    for row in values:
        if row['node_id'] in result and result[row['node_id']]['asn']:
            # Do not update results that contain an ASN,
            # focus on those where it is missing
            continue

        result[row['node_id']] = {
            'asn': row['asn'],
            'total_nodes': row['total_nodes'],
            'nodes_with_identical_asn': row['nodes_with_identical_asn'],
        }
    return result


async def query_crn_measurements(conn: asyncpg.connection, asn_info: Dict,
                                 period: Tuple[datetime, datetime]):
    sql = read_sql_file("query_crn_measurements.template.sql")

    select_last_version = "0.2.5"
    select_previous_version = "0.2.4"
    release_date = datetime.fromisoformat("2022-10-06")
    select_update_deadline = release_date + timedelta(weeks=2)
    select_trusted_owner = ALLOWED_METRICS_SENDER
    select_where_date_gt = period[0]  # datetime.fromisoformat("2022-12-01")
    select_where_date_lt = period[1]  # datetime.fromisoformat("2023-03-10")

    values = await conn.fetch(
        sql,

        select_last_version,
        select_update_deadline,
        select_trusted_owner,
        select_where_date_gt,
        select_where_date_lt,
        select_previous_version,
    )

    for record in values:
        row = dict(record)
        row.update(asn_info[record['node_id']])
        yield record['node_id'], CrnMeasurements.parse_obj(row)


async def compute_crn_scores(period: Tuple[datetime, datetime]) -> List[CrnScore]:
    conn = await asyncpg.connect(
        user="aleph",
        password="569b8f23-0de6-4927-a15d-7157d8583e43",
        database="aleph",
        host="127.0.0.1",
        port=5432,
    )

    asn_info: Dict[str, Dict] = await query_crn_asn_info(conn)

    result = []
    async for node_id, measurements in query_crn_measurements(conn, asn_info, period):

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
        ) ** (1/4)

        if measurements.node_version_missing > (
                measurements.node_version_latest +
                measurements.node_version_outdated +
                measurements.node_version_obsolete
        ) / 5:
            # Too many missing version metrics.
            version_score = 0
        else:
            version_score = (
                    (measurements.node_version_latest + measurements.node_version_outdated) /
                    (measurements.node_version_latest + measurements.node_version_outdated +
                     measurements.node_version_obsolete + measurements.node_version_missing)
            )

        decentralization_score = (
            1 - (measurements.nodes_with_identical_asn / measurements.total_nodes)
        )

        total_score = (
            performance_score * version_score * decentralization_score
        ) ** (1/3)

        result.append(CrnScore(
            node_id=node_id,
            total_score=total_score,
            performance=performance_score,
            version=version_score,
            decentralization=decentralization_score,
            measurements=measurements,
        ))

    await conn.close()

    print("{} CRN nodes with a total score greater than zero".format(
        len([x for x in result if x.total_score > 0])))
    return result


async def query_ccn_asn_info(conn: asyncpg.connection,) -> Dict[str, Dict]:
    """ASN metrics is queried independently of the other metrics
    as to avoid issues related to the group by node_id.
    """
    sql = read_sql_file("query_node_asn_info.template.sql")

    allowed_sender = ALLOWED_METRICS_SENDER
    date_gt = datetime.fromisoformat("2022-12-01")
    date_lt = datetime.fromisoformat("2023-03-10")

    values = await conn.fetch(
        sql,
        allowed_sender,
        date_gt,
        date_lt,
        'ccn',
    )

    result = {}
    for row in values:
        if row['node_id'] in result and result[row['node_id']]['asn']:
            # Do not update results that contain an ASN,
            # focus on those where it is missing
            continue

        result[row['node_id']] = {
            'asn': row['asn'],
            'total_nodes': row['total_nodes'],
            'nodes_with_identical_asn': row['nodes_with_identical_asn'],
        }
    return result


async def query_ccn_measurements(conn: asyncpg.connection, asn_info: Dict,
                                 period: Tuple[datetime, datetime]):
    sql = read_sql_file("query_ccn_measurements.template.sql")

    select_last_version = "v0.4.4"
    select_previous_version = "v0.4.3"
    release_date = datetime.fromisoformat("2023-02-03")
    select_update_deadline = release_date + timedelta(weeks=2)
    select_trusted_owner = ALLOWED_METRICS_SENDER
    select_where_date_gt = period[0]  # datetime.fromisoformat("2022-12-01")
    select_where_date_lt = period[1]  # datetime.fromisoformat("2023-03-10")

    values = await conn.fetch(
        sql,

        select_last_version,
        select_update_deadline,
        select_trusted_owner,
        select_where_date_gt,
        select_where_date_lt,
        select_previous_version,
    )

    for record in values:
        row = dict(record)
        row.update(asn_info[record['node_id']])
        yield record['node_id'], CcnMeasurements.parse_obj(row)


async def compute_ccn_scores(period: Tuple[datetime, datetime]) -> List[CcnScore]:
    conn = await asyncpg.connect(
        user="aleph",
        password="569b8f23-0de6-4927-a15d-7157d8583e43",
        database="aleph",
        host="127.0.0.1",
        port=5432,
    )

    asn_info: Dict[str, Dict] = await query_ccn_asn_info(conn)

    result = []
    async for node_id, measurements in query_ccn_measurements(conn, asn_info, period):

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
        ) ** (1/8)

        if measurements.node_version_missing > (
                measurements.node_version_latest +
                measurements.node_version_outdated +
                measurements.node_version_obsolete
        ) / 5:
            # Too many missing version metrics.
            version_score = 0
        else:
            version_score = (
                    (measurements.node_version_latest + measurements.node_version_outdated) /
                    (measurements.node_version_latest + measurements.node_version_outdated +
                     measurements.node_version_obsolete + measurements.node_version_missing)
            )

        decentralization_score = (
            1 - (measurements.nodes_with_identical_asn / measurements.total_nodes)
        )

        total_score = (
            performance_score * version_score * decentralization_score
        ) ** (1/3)

        result.append(CcnScore(
            node_id=node_id,
            total_score=total_score,
            performance=performance_score,
            version=version_score,
            decentralization=decentralization_score,
            measurements=measurements,
        ))

    await conn.close()

    print("{} CCN nodes with a total score greater than zero".format(
        len([x for x in result if x.total_score > 0])))
    return result


if __name__ == '__main__':
    from_date = datetime.fromisoformat("2022-12-01")
    to_date = datetime.fromisoformat("2023-03-10")
    period = (from_date, to_date)
    ccn_scores = asyncio.run(compute_ccn_scores(period=period))
    crn_scores = asyncio.run(compute_crn_scores(period=period))

    scores = NodeScores(
        ccn=ccn_scores,
        crn=crn_scores,
    )
    with open(Path(__file__).parent.parent.parent / "scores.json", "w") as fd:
        fd.write(scores.json(indent=4))
