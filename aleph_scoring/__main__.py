import time

import schedule
import sentry_sdk
import typer

from .config import settings
from .scoring import compute_node_scores, measure_node_performance

app = typer.Typer()


@app.command()
def run_once():
    measure_node_performance()
    compute_node_scores()


@app.command()
def run_on_schedule():
    schedule.every(3).minutes.at(":00").do(compute_node_scores)
    schedule.every().minutes.at(":00").do(measure_node_performance)

    while True:
        schedule.run_pending()
        time.sleep(1)


def main():
    sentry_sdk.init(
        settings.SENTRY_DSN,
    )
    app()


if __name__ == "__main__":
    main()
