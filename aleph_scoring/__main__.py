import asyncio.exceptions
import logging
import os
import time
from enum import Enum
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import schedule
import sentry_sdk
import typer
from aleph_client.asynchronous import create_post
from aleph_client.chains.ethereum import ETHAccount
from click import BadParameter
from hexbytes import HexBytes

from aleph_scoring.config import settings
from aleph_scoring.metrics import MetricsLogKey, measure_node_performance_sync
from aleph_scoring.schemas.metrics import MetricsPost, NodeMetrics
from aleph_scoring.schemas.scoring import NodeScores, NodeScoresPost
from aleph_scoring.scoring import compute_scores

app = typer.Typer()


logger = logging.getLogger(__name__)
aleph_account: Optional[ETHAccount] = None


class OutputFormat(str, Enum):
    JSON = "json"
    HDF5 = "hdf5"


def save_as_json(node_metrics: NodeMetrics, file: Path):
    with file.open(mode="w") as f:
        f.write(node_metrics.json(indent=4))


def append_metrics_to_file(
    filepath: Path, new_metrics: Dict[MetricsLogKey, pd.DataFrame]
):
    with pd.HDFStore(filepath.as_posix(), mode="a") as store:
        for key, value in new_metrics.items():
            store.put(key, value, format="table", append=True, track_times=False)
        logger.debug(f"Metrics stored in file '{filepath}'")


def save_as_hdf5(node_metrics: NodeMetrics, file: Path):
    new_metrics: Dict[MetricsLogKey, pd.DataFrame] = {
        "core_channel_nodes": pd.DataFrame(
            [metrics.dict() for metrics in node_metrics.ccn]
        ),
        "compute_resource_nodes": pd.DataFrame(
            metrics.dict() for metrics in node_metrics.crn
        ),
    }
    if save_to_file:
        append_metrics_to_file(filepath=file, new_metrics=new_metrics)


def save_to_file(node_metrics: NodeMetrics, file: Path, format: OutputFormat):
    if format == OutputFormat.JSON:
        save_as_json(node_metrics, file)
    elif format == OutputFormat.HDF5:
        save_as_hdf5(node_metrics, file)
    else:
        raise NotImplementedError(f"Unsupported output format: {format}")


def get_aleph_account():
    if not settings.ETHEREUM_PRIVATE_KEY:
        raise ValueError(
            "Could not read Ethereum private key from ETHEREUM_PRIVATE_KEY."
        )

    private_key = HexBytes(settings.ETHEREUM_PRIVATE_KEY)
    account = ETHAccount(private_key)
    return account


async def publish_on_aleph(node_metrics: NodeMetrics, node_scores: NodeScores):
    account = get_aleph_account()

    channel = settings.ALEPH_POST_TYPE_CHANNEL
    aleph_api_server = settings.NODE_DATA_HOST

    metrics_post_data = MetricsPost(tags=["mainnet"], metrics=node_metrics)
    metrics_post = await create_post(
        account=account,
        post_content=metrics_post_data,
        post_type=settings.ALEPH_POST_TYPE_METRICS,
        channel=channel,
        api_server=aleph_api_server,
    )
    logger.debug("Published metrics on Aleph: %s", metrics_post.item_hash)

    scores_post_data = NodeScoresPost(
        tags=["mainnet"], metrics_post=metrics_post.item_hash, scores=node_scores
    )
    scores_post = await create_post(
        account=account,
        post_content=scores_post_data,
        post_type=settings.ALEPH_POST_TYPE_SCORES,
        channel=channel,
        api_server=aleph_api_server,
    )
    logger.debug("Published scores on Aleph: %s", scores_post.item_hash)


def run_scoring(format: OutputFormat, output_file: Optional[Path], post_on_aleph: bool):
    if post_on_aleph and format != OutputFormat.JSON:
        raise BadParameter("Output format must be JSON to post on Aleph.")

    node_metrics = measure_node_performance_sync()
    node_scores = compute_scores(node_metrics)

    if output_file:
        save_to_file(node_metrics=node_metrics, file=output_file, format=format)
    elif format == OutputFormat.JSON:
        print(node_scores.json())

    if post_on_aleph:
        asyncio.run(
            publish_on_aleph(node_metrics=node_metrics, node_scores=node_scores)
        )


@app.command()
def run_once(
    format: OutputFormat = typer.Option(..., help="Output format."),
    output_file: Optional[Path] = typer.Option(None, help="Output file."),
    post_on_aleph: bool = typer.Option(
        False,
        help="Whether to save the results on Aleph. Only usable in combination with --format json.",
    ),
):
    run_scoring(format=format, output_file=output_file, post_on_aleph=post_on_aleph)


@app.command()
def run_n_times(
    n: int = 2,
    format: OutputFormat = typer.Option(..., help="Output format."),
    output_file: Optional[Path] = typer.Option(None, help="Output file."),
    post_on_aleph: bool = typer.Option(
        False,
        help="Whether to save the results on Aleph. Only usable in combination with --format json.",
    ),
):
    """Measure the performance n times."""
    for i in range(n):
        t0 = time.time()

        try:
            run_scoring(
                format=format, output_file=output_file, post_on_aleph=post_on_aleph
            )

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
def run_on_schedule(
    format: OutputFormat = typer.Option(..., help="Output format."),
    output_file: Optional[Path] = typer.Option(None, help="Output file."),
    post_on_aleph: bool = typer.Option(
        False,
        help="Whether to save the results on Aleph. Only usable in combination with --format json.",
    ),
):
    run_scoring(format=format, output_file=output_file, post_on_aleph=post_on_aleph)
    schedule.every(settings.DAEMON_MODE_PERIOD_HOURS).hours.at(":00").do(
        run_scoring,
        format=format,
        output_file=output_file,
        post_on_aleph=post_on_aleph,
    )

    logger.debug("Running the scheduler")
    while True:
        schedule.run_pending()
        time.sleep(1)


@app.command()
def export_as_html(input_file: Optional[Path]):
    os.system("jupyter nbconvert --execute Node\\ Score\\ Analysis.ipynb --to html")


def main():
    logging.basicConfig(level=settings.LOGGING_LEVEL)
    if settings.SENTRY_DSN:
        sentry_sdk.init(
            settings.SENTRY_DSN,
        )
    app()


if __name__ == "__main__":
    main()
