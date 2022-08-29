import asyncio.exceptions
import logging
import os
import time
from pathlib import Path
from typing import Optional

import schedule
import sentry_sdk
import typer

from aleph_scoring.config import settings
from aleph_scoring.scoring import measure_node_performance_sync

app = typer.Typer()


logger = logging.getLogger(__name__)


@app.command()
def run_once(save_to_file: Optional[Path] = None):
    measure_node_performance_sync(save_to_file=save_to_file)


@app.command()
def run_n_times(n: int = 2, save_to_file: Optional[Path] = None):
    """Measure the performance n times."""
    for i in range(n):
        t0 = time.time()

        try:
            measure_node_performance_sync(save_to_file=save_to_file)

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
def run_on_schedule(save_to_file: Optional[Path] = None):
    measure_node_performance_sync(save_to_file=save_to_file)
    schedule.every(1).minutes.at(":00").do(
        measure_node_performance_sync, save_to_file=save_to_file
    )
    # schedule.every(3).minutes.at(":00").do(compute_node_scores)

    logger.debug("Running the scheduler")
    while True:
        schedule.run_pending()
        time.sleep(1)


@app.command()
def export_as_html(input_file: Optional[Path]):
    os.system("jupyter nbconvert --execute Node\\ Score\\ Analysis.ipynb --to html")


def main():
    logging.basicConfig(level=settings.LOGGING_LEVEL)
    sentry_sdk.init(
        settings.SENTRY_DSN,
    )
    app()


if __name__ == "__main__":
    main()
