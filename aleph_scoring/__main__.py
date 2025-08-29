import asyncio.exceptions
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import schedule
import sentry_sdk
import typer
from aleph.sdk.chains.ethereum import ETHAccount
from aleph.sdk.client import AuthenticatedAlephHttpClient
from aleph.sdk.types import Account
from hexbytes import HexBytes
from pydantic import BaseModel

from aleph_scoring.config import settings
from aleph_scoring.executions import record_node_executions_sync
from aleph_scoring.executions.models import ExecutionsPost, NodeExecutions
from aleph_scoring.metrics import measure_node_performance_sync
from aleph_scoring.metrics.models import MetricsPost, NodeMetrics
from aleph_scoring.scoring import compute_ccn_scores, compute_crn_scores
from aleph_scoring.scoring.models import NodeScores, NodeScoresPost
from aleph_scoring.utils import LogLevel, Period, database_connection

logger = logging.getLogger(__name__)
aleph_account: Optional[ETHAccount] = None

app = typer.Typer()


def save_as_json(node_metrics: BaseModel, file: Path):
    with file.open(mode="w") as f:
        f.write(node_metrics.json(indent=4))


def ensure_private_key_available():
    """Ensure that the Ethereum private key is available.

    This can be done early, before running time-consuming operations.
    """
    if not settings.ETHEREUM_PRIVATE_KEY and not settings.ETHEREUM_PRIVATE_KEY_PATH:
        raise ValueError(
            "Could not read Ethereum private key from ETHEREUM_PRIVATE_KEY or ETHEREUM_PRIVATE_KEY_PATH."
        )
    if (
        settings.ETHEREUM_PRIVATE_KEY_PATH
        and not settings.ETHEREUM_PRIVATE_KEY_PATH.exists()
    ):
        raise ValueError(
            f"Could not read Ethereum private key from ETHEREUM_PRIVATE_KEY_PATH: {settings.ETHEREUM_PRIVATE_KEY_PATH}"
        )


def get_aleph_account():
    private_key_str: str
    if settings.ETHEREUM_PRIVATE_KEY_PATH:
        private_key_str = settings.ETHEREUM_PRIVATE_KEY_PATH.read_text().strip()
    elif settings.ETHEREUM_PRIVATE_KEY:
        private_key_str = settings.ETHEREUM_PRIVATE_KEY
    else:
        raise ValueError(
            "Could not read Ethereum private key from ETHEREUM_PRIVATE_KEY."
        )

    private_key = HexBytes(private_key_str)
    account = ETHAccount(private_key)
    return account


async def publish_metrics_on_aleph(account: Account, node_metrics: NodeMetrics):
    channel = settings.ALEPH_POST_TYPE_CHANNEL
    aleph_api_server = settings.NODE_DATA_HOST

    metrics_post_data = MetricsPost(tags=["mainnet"], metrics=node_metrics)
    async with AuthenticatedAlephHttpClient(
        account=account, api_server=aleph_api_server
    ) as client:
        metrics_post, status = await client.create_post(
            post_content=metrics_post_data,
            post_type=settings.ALEPH_POST_TYPE_METRICS,
            channel=channel,
        )
    logger.info(
        "Published metrics on Aleph with status %s: %s", status, metrics_post.item_hash
    )


async def publish_scores_on_aleph(
    account: Account, node_scores: NodeScores, period: Period
):
    channel = settings.ALEPH_POST_TYPE_CHANNEL
    aleph_api_server = settings.NODE_DATA_HOST

    scores_post_data = NodeScoresPost(
        tags=["mainnet"],
        scores=node_scores,
        period=period,
        measured_at=datetime.now(tz=timezone.utc).isoformat(),
    )

    post_content = scores_post_data.dict()
    # Force datetime conversion to string
    post_content["period"] = json.loads(period.json())

    async with AuthenticatedAlephHttpClient(
        account=account, api_server=aleph_api_server
    ) as client:
        scores_post, status = await client.create_post(
            post_content=post_content,
            post_type=settings.ALEPH_POST_TYPE_SCORES,
            channel=channel,
        )
    logger.info(
        "Published scores on Aleph with status %s: %s", status, scores_post.item_hash
    )


async def get_latest_metrics() -> dict:
    sql = """
        SELECT 
            *
        FROM posts
        WHERE
            owner = $1
            AND type = $2
        ORDER BY
            creation_datetime DESC
        LIMIT 1
    """
    conn = await database_connection(settings)
    messages = await conn.fetch(
        sql,
        settings.ALLOWED_METRICS_SENDER,
        settings.ALEPH_POST_TYPE_METRICS,
    )
    # print(messages)
    return messages[0]


async def get_latest_metrics_age(now: datetime) -> timedelta:
    """
    Get the age of the most recent metrics from the specified `pyaleph` node.
    """
    latest_metrics_message: dict = await get_latest_metrics()
    creation_datetime: datetime = latest_metrics_message["creation_datetime"]
    return now - creation_datetime


def run_measurements(
    output: Optional[Path] = typer.Option(
        default=None, help="Path where to save the result in JSON format."
    ),
    stdout: bool = typer.Option(default=False, help="Print the result on stdout"),
    publish: bool = typer.Option(
        default=False,
        help="Publish the results on Aleph.",
    ),
):
    node_metrics = measure_node_performance_sync()

    if output:
        save_as_json(node_metrics=node_metrics, file=output)
    if stdout:
        print(node_metrics.json(indent=4))
    if publish:
        account = get_aleph_account()
        asyncio.run(
            publish_metrics_on_aleph(account=account, node_metrics=node_metrics)
        )


@app.command()
def measure(
    output: Optional[Path] = typer.Option(
        default=None, help="Path where to save the result in JSON format."
    ),
    publish: bool = typer.Option(
        default=False,
        help="Publish the results on Aleph.",
    ),
    log_level: str = typer.Option(
        default=LogLevel.INFO.name,
        help="Logging level",
    ),
):
    logging.basicConfig(level=LogLevel[log_level].value)
    if publish:
        ensure_private_key_available()
    run_measurements(output=output, publish=publish)


async def publish_executions_on_aleph(
    account: Account, node_executions: NodeExecutions
):
    channel = settings.ALEPH_POST_TYPE_CHANNEL
    aleph_api_server = settings.NODE_DATA_HOST

    metrics_post_data = ExecutionsPost(tags=["mainnet"], metrics=node_executions)
    async with AuthenticatedAlephHttpClient(
        account=account, api_server=aleph_api_server
    ) as client:
        metrics_post, status = await client.create_post(
            post_content=metrics_post_data,
            post_type=settings.ALEPH_POST_TYPE_EXECUTIONS,
            channel=channel,
        )
    logger.info(
        "Published executions on Aleph with status %s: %s",
        status,
        metrics_post.item_hash,
    )


@app.command()
def record_executions(
    output: Optional[Path] = typer.Option(
        default=None, help="Path where to save the result in JSON format."
    ),
    publish: bool = typer.Option(
        default=False,
        help="Publish the results on Aleph.",
    ),
    stdout: bool = typer.Option(default=False, help="Print the result on stdout"),
    log_level: str = typer.Option(
        default=LogLevel.INFO.name,
        help="Logging level",
    ),
):
    logging.basicConfig(level=LogLevel[log_level].value)
    if publish:
        ensure_private_key_available()

    node_executions = record_node_executions_sync()

    if output:
        save_as_json(node_metrics=node_executions, file=output)
    if stdout:
        print(node_executions.json(indent=4))
    if publish:
        account = get_aleph_account()
        asyncio.run(
            publish_executions_on_aleph(
                account=account, node_executions=node_executions
            )
        )


@app.command()
def measure_on_schedule(
    output: Optional[Path] = typer.Option(
        default=None, help="Path where to save the result in JSON format."
    ),
    publish: bool = typer.Option(
        default=False,
        help="Publish the results on Aleph.",
    ),
    log_level: str = typer.Option(
        default=LogLevel.INFO.name,
        help="Logging level",
    ),
):
    logging.basicConfig(level=LogLevel[log_level].value)
    if publish:
        ensure_private_key_available()
    compute_scores(output=output, publish=publish, log_level=log_level)

    schedule.every(settings.DAEMON_MODE_PERIOD_HOURS).hours.at(":00").do(
        compute_scores,
        save=output,
        publish=publish,
        log_level=log_level,
    )

    logger.debug("Running the scheduler")
    while True:
        schedule.run_pending()
        time.sleep(1)


@app.command()
def measure_n_times(
    n: int = 2,
    output: Optional[Path] = typer.Option(
        default=None, help="Path where to save the result in JSON format."
    ),
    publish: bool = typer.Option(
        default=False,
        help="Publish the results on Aleph.",
    ),
    log_level: str = typer.Option(
        default=LogLevel.INFO.name,
        help="Logging level",
    ),
):
    """Measure the performance n times."""

    logging.basicConfig(level=LogLevel[log_level].value)
    if publish:
        ensure_private_key_available()

    for i in range(n):
        t0 = time.time()

        try:
            run_measurements(output=output, publish=publish)

            duration = time.time() - t0
            delay = max(60 - duration, 0)
            logger.debug(
                f"Waiting for {delay:.2f} seconds before measurement {i + 1}/{n}..."
            )
            time.sleep(delay)
        except asyncio.exceptions.TimeoutError:
            logger.warning(
                "Node info could not be fetched. Retrying in 5 seconds...",
                exc_info=True,
            )
            time.sleep(5)


@app.command()
def compute_scores(
    output: Optional[Path] = typer.Option(
        default=None, help="Path where to save the result in JSON format."
    ),
    stdout: bool = typer.Option(default=False, help="Print the result on stdout"),
    publish: bool = typer.Option(
        default=False,
        help="Publish the results on Aleph.",
    ),
    log_level: str = typer.Option(
        default=LogLevel.INFO.name,
        help="Logging level",
    ),
):
    logging.basicConfig(level=LogLevel[log_level].value)
    if publish:
        ensure_private_key_available()

    to_date = datetime.now(tz=timezone.utc)
    from_date = to_date - settings.SCORE_METRICS_PERIOD
    current_period = Period(from_date=from_date, to_date=to_date)

    logger.info(
        f"Period = {current_period.from_date.isoformat()} to {current_period.to_date.isoformat()}"
    )

    # Ensure that recent metrics are available on the node before computing scores
    latest_metrics_age: timedelta = asyncio.run(get_latest_metrics_age(to_date))
    if latest_metrics_age > settings.MAX_METRICS_AGE:
        logger.error(
            "The most recent metrics are too old: %s. Check if the node has pending messages.",
            latest_metrics_age,
        )
        raise typer.Exit(2)

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

    if stdout or output:
        result = scores.json(indent=4)
        if stdout:
            print(result)
        if output:
            with open(output, "w") as fd:
                fd.write(result)

    if publish:
        account = get_aleph_account()
        asyncio.run(publish_scores_on_aleph(account, scores, current_period))


@app.command()
def compute_on_schedule(
    output: Optional[Path] = typer.Option(
        default=None, help="Path where to save the result in JSON format."
    ),
    publish: bool = typer.Option(
        default=False,
        help="Publish the results on Aleph.",
    ),
    log_level: str = typer.Option(
        default=LogLevel.INFO.name,
        help="Logging level",
    ),
):
    logging.basicConfig(level=LogLevel[log_level].value)
    if publish:
        ensure_private_key_available()

    compute_scores(output=output, publish=publish, log_level=log_level)

    schedule.every(settings.DAEMON_MODE_PERIOD_HOURS).hours.at(":00").do(
        compute_scores,
        save=output,
        publish=publish,
        log_level=log_level,
    )

    logger.debug("Running the scheduler")
    while True:
        schedule.run_pending()
        time.sleep(1)


@app.command()
def export_as_html(input_file: Optional[Path]):
    os.system("jupyter nbconvert --execute Node\\ Score\\ Analysis.ipynb --to html")


def get_sentry_dsn(app_settings):
    if app_settings.SENTRY_DSN:
        return app_settings.SENTRY_DSN

    sentry_dsn_path = app_settings.SENTRY_DSN_PATH
    if sentry_dsn_path and sentry_dsn_path.is_file():
        return sentry_dsn_path.read_text().strip()


def main():
    sentry_dsn: str = get_sentry_dsn(settings)
    sentry_sdk.init(sentry_dsn)
    app()


if __name__ == "__main__":
    main()
