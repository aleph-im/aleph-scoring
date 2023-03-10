import asyncio
from datetime import datetime
from pathlib import Path

import asyncpg

METRICS_EMITTER = "0x4d741d44348B21e97000A8C9f07Ee34110F7916F"


async def run():
    conn = await asyncpg.connect(
        user="aleph",
        password="569b8f23-0de6-4927-a15d-7157d8583e43",
        database="aleph",
        host="127.0.0.1",
        port=5432,
    )

    with open(Path(__file__).parent / "sql/04.template.sql") as fd:
        sql = fd.read()

    stmt = await conn.prepare(sql)

    select_last_version = "0.2.5"
    select_last_release_date = datetime.fromisoformat("2022-10-06")
    select_trusted_owner = "0x4d741d44348B21e97000A8C9f07Ee34110F7916F"
    select_where_date_gt = datetime.fromisoformat("2023-01-30")
    select_where_date_lt = datetime.fromisoformat("2023-02-21")
    values = await conn.fetch(
        sql,
        select_last_version,
        select_last_release_date,
        select_trusted_owner,
        select_where_date_gt,
        select_where_date_lt,
    )

    for row in values:
        print(dict(row))

    # print(values)
    await conn.close()


asyncio.run(run())
