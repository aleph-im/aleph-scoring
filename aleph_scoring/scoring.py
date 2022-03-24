import asyncio
import json
import logging
import re
import time
from asyncio.log import logger
from datetime import datetime
from typing import Any, Dict, Generator, Optional, Tuple
from urllib.request import urlopen

import aiohttp
import async_timeout
import pandas as pd
from pydantic import BaseModel

from .config import settings


# Global variables
ccn_df: pd.DataFrame = None
crn_df: pd.DataFrame = None

CCN_AGGREGATE_PATH = (
    "{url}api/v0/aggregates/0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10.json"
    "?keys=corechannel&limit=50"
)

CCN_FILE_DOWNLOAD_PATH = (
    "{url}api/v0/storage/raw/"
    "50645d4ccfddb7540e7bb17ffa5609ec8a980e588e233f0e2c4451f6f9da6ebd"
)

CRN_DIAGNOSTIC_VM_PATH = (
    "{url}vm/67705389842a0a1b95eaa408b009741027964edc805997475e95c505d642edd8",
)


def get_api_node_urls(
    raw_data: Dict[str, Any]
) -> Generator[Dict[str, str], None, None]:
    for node in raw_data["data"]["corechannel"]["nodes"]:
        multiaddress = node["multiaddress"]
        match = re.findall(r"/ip4/([\d\\.]+)/.*", multiaddress)
        if match:
            ip = match[0]
            yield {"url": f"http://{ip}:4024/"}


def get_compute_resource_node_urls(
    raw_data: Dict[str, Any]
) -> Generator[Dict[str, str], None, None]:
    for node in raw_data["data"]["corechannel"]["resource_nodes"]:
        addr = node["address"].strip("/")
        if addr:
            if not addr.startswith("https://"):
                addr = "https://" + addr
            yield {"url": addr + "/"}


async def execute_request(
    session: aiohttp.ClientSession,
    url: str,
    timeout_limit: int,
    return_json: bool = False,
    expected_status: int = 200,
) -> Tuple[Optional[float], Optional[Any]]:
    try:
        async with async_timeout.timeout(timeout_limit):
            start = time.time()
            async with session.get(url) as resp:
                if resp.status != expected_status:
                    raise aiohttp.ClientResponseError(
                            resp.request_info,
                            resp.history,
                            status=resp.status,
                            message="Wrong status code",
                        )
                if return_json:
                    json_text = await resp.json()
                    end = time.time()
                    return end - start, json_text
                else:
                    await resp.release()
                    end = time.time()
                    return end - start, None
    except aiohttp.ClientResponseError:
        logger.warning(f"Error when fetching {url}")
        return None, None
    except aiohttp.ClientConnectorError:
        logger.warning(f"Error when fetching {url}")
        return None, None
    except asyncio.TimeoutError:
        logger.warning(f"Timeout error when fetching {url}")
        return None, None


# Pydantic classe to parse json to object
class MetricJson(BaseModel):
    pyaleph_status_sync_pending_txs_total: str = "NaN"
    pyaleph_status_sync_pending_messages_total: str = "NaN"
    pyaleph_status_chain_eth_height_remaining_total: str = "NaN"

    class Config:
        allow_population_by_field_name = True


async def get_ccn_metrics(session: aiohttp.ClientSession, url: str) -> dict:
    base_latency = (
        await execute_request(
            session, f"{url}api/v0/info/public.json",
            settings.TIMEOUT_LIMIT_CCN
        )
    )[0]
    metrics_latency = (
        await execute_request(session, f"{url}metrics.json",
                              settings.TIMEOUT_LIMIT_CCN)
    )[0]
    aggregate_latency = (
        await execute_request(
            session,
            "".join(CCN_AGGREGATE_PATH).format(url=url),
            settings.TIMEOUT_LIMIT_CCN,
        )
    )[0]
    file_download_latency = (
        await execute_request(
            session,
            "".join(CCN_FILE_DOWNLOAD_PATH).format(url=url),
            settings.TIMEOUT_LIMIT_CCN,
        )
    )[0]
    time, json_text = await execute_request(
        session, f"{url}metrics.json", settings.TIMEOUT_LIMIT_CCN, True
    )

    if json_text is not None:
        json_object = MetricJson(**json_text)
    else:
        json_object = MetricJson()

    metrics = {
        "url": url,
        "base_latency": base_latency,
        "metrics_latency": metrics_latency,
        "aggregate_latency": aggregate_latency,
        "file_download_latency": file_download_latency,
        "txs_total": json_object.pyaleph_status_sync_pending_txs_total,
        "pending_messages": json_object.pyaleph_status_sync_pending_messages_total,  # noqa:E501
        "eth_height_remaining": json_object.pyaleph_status_chain_eth_height_remaining_total,  # noqa:E501
    }

    return metrics


async def get_crn_metrics(session: aiohttp.ClientSession, url: str) -> dict:
    base_latency = (
        await execute_request(
            session,
            f"{url}about/login",
            settings.TIMEOUT_LIMIT_CRN,
            expected_status=401,
        )
    )[0]

    diagnostic_VM_latency = (
        await execute_request(
            session,
            "".join(CRN_DIAGNOSTIC_VM_PATH).format(url=url),
            settings.TIMEOUT_LIMIT_CRN,
        )
    )[0]
    full_check_latency = (
        await execute_request(
            session, f"{url}status/check/fastapi", settings.TIMEOUT_LIMIT_CRN
        )
    )[0]

    metrics = {
        "url": url,
        "base_latency": base_latency,
        "diagnostic_VM_latency": diagnostic_VM_latency,
        "full_check_latency": full_check_latency,
    }

    return metrics


async def compute_metrics_async(is_ccn: bool):

    url = (
        "https://api2.aleph.im/api/v0/aggregates/"
        "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10.json?"
        "keys=corechannel&limit=50"
    )
    jsonurl = urlopen(url)
    text = json.loads(jsonurl.read())

    async with aiohttp.ClientSession() as session:
        if is_ccn:
            tasks = [
                get_ccn_metrics(session, item["url"])
                for item in get_api_node_urls(text)
            ]
        else:
            tasks = [
                get_crn_metrics(session, item["url"])
                for item in get_compute_resource_node_urls(text)
            ]
        return await asyncio.gather(*tasks)


def measure_node_performance():

    logging.basicConfig(level=settings.LOGGING_LEVEL)
    logger = logging.getLogger(__name__)

    start_time = time.time()

    df = get_metrics_async(False)

    logger.info(
        f"Finished gathering crn metrics in {(time.time() - start_time)} seconds"  # noqa:E501
    )

    global crn_df
    if crn_df is None:
        crn_df = df
    else:
        crn_df = pd.concat([crn_df, df], ignore_index=True)

    if settings.EXPORT_DATAFRAME:
        crn_df.to_csv(f"exports/crn_metrics-{datetime.now()}")

    start_time = time.time()

    df = get_metrics_async(True)

    logger.info(
        f"Finished gathering ccn metrics in {(time.time() - start_time)} seconds"  # noqa:E501
    )

    global ccn_df
    if ccn_df is None:
        ccn_df = df
    else:
        ccn_df = pd.concat([ccn_df, df], ignore_index=True)

    if settings.EXPORT_DATAFRAME:
        ccn_df.to_csv(f"exports/ccn_metrics-{datetime.now()}")


def get_metrics_async(is_ccn: bool) -> pd.DataFrame:
    loop = asyncio.get_event_loop()

    metrics = loop.run_until_complete(compute_metrics_async(is_ccn))

    df = pd.DataFrame(metrics)

    return df


def compute_global_score(is_ccn: bool):

    if is_ccn:
        global ccn_df
        hourly_df = ccn_df
        ccn_df = None
    else:
        global crn_df
        hourly_df = crn_df
        crn_df = None

    cols = [i for i in hourly_df.columns if i not in ["url"]]
    for col in cols:
        hourly_df[col] = pd.to_numeric(hourly_df[col], errors="coerce")
    hourly_df = hourly_df.fillna(1)

    if is_ccn:
        global_score_ccn(hourly_df)
    else:
        global_score_crn(hourly_df)


def global_score_ccn(df: pd.DataFrame):
    "Compute the global score of a Core Channel Node (CCN)"

    df = df.groupby("url").agg(
        {
            "base_latency": "mean",
            "metrics_latency": "mean",
            "aggregate_latency": "mean",
            "file_download_latency": "mean",
            "txs_total": "mean",
            "pending_messages": "mean",
            "eth_height_remaining": "mean",
        }
    )

    df["score"] = (
        df["base_latency"]
        * df["metrics_latency"]
        * df["aggregate_latency"]
        * df["file_download_latency"]
        * df["txs_total"]
        * df["pending_messages"]
        * df["eth_height_remaining"]
        / 1000
    )

    if settings.EXPORT_DATAFRAME:
        df.to_csv(f"exports/ccn_score-{datetime.now()}")

    logger.info("Finished processing ccn score")


def global_score_crn(df: pd.DataFrame):
    "Compute the global score of a Core Channel Node (CCN)"

    df = df.groupby("url").agg(
        {
            "base_latency": "mean",
            "diagnostic_VM_latency": "mean",
            "full_check_latency": "mean",
        }
    )

    df["score"] = (
        df["base_latency"]
        * df["diagnostic_VM_latency"]
        * df["full_check_latency"]
        / 1000
    )

    if settings.EXPORT_DATAFRAME:
        df.to_csv(f"exports/crn_score-{datetime.now()}")

    logger.info("Finished processing crn score ")


def compute_node_scores():
    compute_global_score(True)
    compute_global_score(False)
