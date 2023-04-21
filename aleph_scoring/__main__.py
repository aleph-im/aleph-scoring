import asyncio.exceptions
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import schedule
import sentry_sdk
import typer
from aleph.sdk.chains.ethereum import ETHAccount
from aleph.sdk.client import AuthenticatedAlephClient
from aleph.sdk.types import Account
from hexbytes import HexBytes

from aleph_scoring.config import settings
from aleph_scoring.metrics import measure_node_performance_sync
from aleph_scoring.metrics.models import MetricsPost, NodeMetrics
from aleph_scoring.scoring import compute_ccn_scores, compute_crn_scores
from aleph_scoring.scoring.models import NodeScores, NodeScoresPost
from aleph_scoring.utils import LogLevel, Period, get_latest_github_releases

logger = logging.getLogger(__name__)
aleph_account: Optional[ETHAccount] = None

app = typer.Typer()


def save_as_json(node_metrics: NodeMetrics, file: Path):
    with file.open(mode="w") as f:
        f.write(node_metrics.json(indent=4))


def get_aleph_account():
    if not settings.ETHEREUM_PRIVATE_KEY:
        raise ValueError(
            "Could not read Ethereum private key from ETHEREUM_PRIVATE_KEY."
        )

    private_key = HexBytes(settings.ETHEREUM_PRIVATE_KEY)
    account = ETHAccount(private_key)
    return account


async def publish_metrics_on_aleph(account: Account, node_metrics: NodeMetrics):
    channel = settings.ALEPH_POST_TYPE_CHANNEL
    aleph_api_server = settings.NODE_DATA_HOST

    metrics_post_data = MetricsPost(tags=["mainnet"], metrics=node_metrics)
    async with AuthenticatedAlephClient(
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
    )

    post_content = scores_post_data.dict()
    # Force datetime conversion to string
    post_content["period"] = json.loads(period.json())

    async with AuthenticatedAlephClient(
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
    logging.basicConfig(level=LogLevel[log_level])
    run_measurements(output=output, publish=publish)


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
    logging.basicConfig(level=LogLevel[log_level])
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

    logging.basicConfig(level=LogLevel[log_level])

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
    logging.basicConfig(level=LogLevel[log_level])

    to_date = datetime.utcnow()
    from_date = to_date - settings.SCORE_METRICS_PERIOD
    current_period = Period(from_date=from_date, to_date=to_date)

    # (
    #     latest_ccn_release,
    #     previous_ccn_release,
    #     latest_ccn_prerelease,
    # ) = get_latest_github_releases("aleph-im", "pyaleph")
    # (
    #     latest_crn_release,
    #     previous_crn_release,
    #     latest_crn_prerelease,
    # ) = get_latest_github_releases("aleph-im", "aleph-vm")

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
    logging.basicConfig(level=LogLevel[log_level])
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


def main():
    if settings.SENTRY_DSN:
        sentry_sdk.init(
            settings.SENTRY_DSN,
        )
    app()


if __name__ == "__main__":
    main()
