import asyncio
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, Optional, Tuple, Callable, Iterable, Union
from typing import Literal

import aiohttp
import async_timeout
import numpy
import pandas as pd
from pandas import DataFrame
from pydantic import BaseModel, validator
from urllib3.util import Url, parse_url

from .config import settings

logger = logging.getLogger(__name__)

# Global variable used to aggregate the metrics over time
metrics_log = {"core_channel_nodes": None, "compute_resource_nodes": None}
MetricsLogKey = Literal["core_channel_nodes", "compute_resource_nodes"]

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


class NodeInfo(BaseModel):
    url: Url
    hash: str

    @validator('hash')
    def hash_format(cls, v) -> str:
        if len(v) != 64:
            raise ValueError("must have a length of 64")
        try:
            # Parse as hexadecimal using int()
            int(v, 16)
        except ValueError:
            raise ValueError("must be hexadecimal")
        return v


def get_api_node_urls(
    raw_data: Dict[str, Any]
) -> Generator[NodeInfo, None, None]:
    """Extract CCN urls from node data."""
    for node in raw_data["data"]["corechannel"]["nodes"]:
        multiaddress = node["multiaddress"]
        match = re.findall(r"/ip4/([\d\\.]+)/.*", multiaddress)
        if match:
            ip = match[0]
            yield NodeInfo(
                url=parse_url(f"http://{ip}:4024/"),
                hash=node["hash"],
            )


def get_compute_resource_node_urls(
    raw_data: Dict[str, Any]
) -> Generator[NodeInfo, None, None]:
    """Extract CRN node urls the node data."""
    for node in raw_data["data"]["corechannel"]["resource_nodes"]:
        addr = node["address"].strip("/")
        if addr:
            if not addr.startswith("https://"):
                addr = "https://" + addr
            yield NodeInfo(
                url=parse_url(addr + "/"),
                hash=node["hash"],
            )


async def measure_http_latency(
    session: aiohttp.ClientSession,
    url: str,
    timeout_seconds=settings.HTTP_REQUEST_TIMEOUT,
    return_json: bool = False,
    expected_status: int = 200,
) -> Tuple[Optional[float], Optional[Any]]:
    try:
        async with async_timeout.timeout(timeout_seconds):
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
                    logger.debug(f"Success when fetching {url}")
                    return end - start, json_text
                else:
                    await resp.release()
                    end = time.time()
                    logger.debug(f"Success when fetching {url}")
                    return end - start, None
    except aiohttp.ClientResponseError:
        logger.debug(f"Error when fetching {url}")
        return None, None
    except aiohttp.ClientConnectorError:
        logger.debug(f"Error when fetching {url}")
        return None, None
    except asyncio.TimeoutError:
        logger.debug(f"Timeout error when fetching {url}")
        return None, None


# Pydantic class to parse json to object
class CCNMetrics(BaseModel):
    pyaleph_status_sync_pending_txs_total: Union[int, float] = numpy.NaN
    pyaleph_status_sync_pending_messages_total: Union[int, float] = numpy.NaN
    pyaleph_status_chain_eth_height_remaining_total: Union[int, float] = numpy.NaN

    class Config:
        allow_population_by_field_name = True


async def get_ccn_metrics(session: aiohttp.ClientSession, node_info: NodeInfo) -> dict:
    url = node_info.url.url
    measured_at = datetime.utcnow()
    base_latency = (
        await measure_http_latency(session, f"{url}api/v0/info/public.json")
    )[0]
    metrics_latency = (
        await measure_http_latency(
            session, f"{url}metrics.json", settings.HTTP_REQUEST_TIMEOUT
        )
    )[0]
    aggregate_latency = (
        await measure_http_latency(
            session,
            "".join(CCN_AGGREGATE_PATH).format(url=url),
        )
    )[0]
    file_download_latency = (
        await measure_http_latency(
            session,
            "".join(CCN_FILE_DOWNLOAD_PATH).format(url=url),
        )
    )[0]
    time, json_text = await measure_http_latency(
        session, f"{url}metrics.json", settings.HTTP_REQUEST_TIMEOUT, True
    )

    if json_text is not None:
        json_object = CCNMetrics(**json_text)
    else:
        json_object = CCNMetrics()

    metrics = {
        "measured_at": measured_at,
        "node_id": node_info.hash,
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


async def get_crn_metrics(session: aiohttp.ClientSession, node_info: NodeInfo) -> dict:
    url = node_info.url.url
    measured_at = datetime.utcnow()
    base_latency = (
        await measure_http_latency(
            session,
            f"{url}about/login",
            expected_status=401,
        )
    )[0]

    diagnostic_VM_latency = (
        await measure_http_latency(
            session,
            "".join(CRN_DIAGNOSTIC_VM_PATH).format(url=url),
            timeout_seconds=10,
        )
    )[0]
    full_check_latency = (
        await measure_http_latency(
            session,
            f"{url}status/check/fastapi",
            timeout_seconds=20,
        )
    )[0]

    metrics = {
        "measured_at": measured_at,
        "node_id": node_info.hash,
        "url": url,
        "base_latency": base_latency,
        "diagnostic_VM_latency": diagnostic_VM_latency,
        "full_check_latency": full_check_latency,
    }

    return metrics


async def collect_node_metrics(
    node_infos: Iterable[NodeInfo], metrics_function: Callable
):
    timeout = aiohttp.ClientTimeout(
        total=60.0, connect=2.0, sock_connect=2.0, sock_read=60.0
    )
    async with aiohttp.ClientSession(timeout=timeout) as session:
        return await asyncio.gather(
            *[metrics_function(session, node_info) for node_info in node_infos]
        )


async def collect_all_ccn_metrics(node_data: Dict[str, Any]):
    node_infos = get_api_node_urls(node_data)
    return await collect_node_metrics(
        node_infos=node_infos, metrics_function=get_ccn_metrics
    )


async def collect_all_crn_metrics(node_data: Dict[str, Any]):
    node_infos = get_compute_resource_node_urls(node_data)
    return await collect_node_metrics(
        node_infos=node_infos, metrics_function=get_crn_metrics
    )


async def get_aleph_nodes() -> Dict:
    url = settings.node_data_url
    timeout = aiohttp.ClientTimeout(total=10.0)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        resp = await session.get(url)
        resp.raise_for_status()
        return await resp.json()


async def collect_all_node_metrics():
    aleph_nodes = await get_aleph_nodes()
    logger.debug("Fetched node data")
    ccn_metrics = await collect_all_ccn_metrics(aleph_nodes)
    logger.debug("Fetched CCN metrics")
    crn_metrics = await collect_all_crn_metrics(aleph_nodes)
    logger.debug("Fetched CRN metrics")
    return ccn_metrics, crn_metrics


def update_metrics_log(metrics_log, key: MetricsLogKey, new_metrics: DataFrame):
    previous_metrics = metrics_log[key]
    if previous_metrics is None:
        metrics_log[key] = new_metrics
    else:
        metrics_log[key] = pd.concat((previous_metrics, new_metrics), ignore_index=True)


def save_metrics_log(metrics_log):
    store = pd.HDFStore("/tmp/store.h5")
    try:
        for key, value in metrics_log.items():
            store[key] = value
    finally:
        store.close()


def append_metrics_to_file(filepath: Path, new_metrics: Dict[MetricsLogKey, DataFrame]):
    store = pd.HDFStore(filepath.as_posix(), mode="a")
    try:
        for key, value in new_metrics.items():
            store.put(key, value, format="table", append=True, track_times=False)
        logger.debug(f"Metrics stored in file '{filepath}'")
    finally:
        store.close()


async def measure_node_performance(save_to_file: Optional[Union[Path, str]] = None):
    logger.debug("Measuring node performance")

    if isinstance(save_to_file, str):
        save_to_file = Path(save_to_file)

    ccn_metrics, crn_metrics = await collect_all_node_metrics()

    new_metrics: Dict[MetricsLogKey, DataFrame] = {
        "core_channel_nodes": DataFrame(ccn_metrics),
        "compute_resource_nodes": DataFrame(crn_metrics),
    }
    if save_to_file:
        append_metrics_to_file(filepath=save_to_file, new_metrics=new_metrics)


def measure_node_performance_sync(save_to_file: Optional[Path] = None):
    return asyncio.run(measure_node_performance(save_to_file=save_to_file))


def compute_ccn_score(df: DataFrame):
    "Compute the global score of a Core Channel Node (CCN)"

    scores = df.copy()

    def score_latency(value):
        value = max(min(value, value - 0.10), 0)  # Tolerance
        score = max(1 - value, 0)
        assert 0 <= score <= 1, f"Out of range {score} from {value}"
        return score

    def score_pending_messages(value):
        if value == "NaN":
            value = 1_000_000
        value = int(value)
        return 1 - (value / 1_000_000)

    def score_eth_height_remaining(value):
        if value == "NaN":
            value = 100_000
        value = max(int(value), 0)
        return 1 - (value / 100_000)

    scores["score_base_latency"] = df["base_latency"].fillna(10).apply(score_latency)
    scores["score_metrics_latency"] = (
        df["metrics_latency"].fillna(10).apply(score_latency)
    )
    scores["score_aggregate_latency"] = (
        df["aggregate_latency"].fillna(10).apply(score_latency)
    )
    scores["score_file_download_latency"] = (
        df["file_download_latency"].fillna(10).apply(score_latency)
    )
    scores["score_pending_messages"] = (
        df["pending_messages"].fillna(1_000_000).apply(score_pending_messages)
    )
    scores["score_eth_height_remaining"] = (
        df["eth_height_remaining"].fillna(100_000).apply(score_eth_height_remaining)
    )

    scores["score"] = (
        scores["score_base_latency"]
        * scores["score_metrics_latency"]
        * scores["score_aggregate_latency"]
        * scores["score_file_download_latency"]
        * scores["score_pending_messages"]
        # * scores["eth_height_remaining"]
    ) ** (1 / 5.0)
    return scores


def compute_crn_score(df: DataFrame):
    "Compute the global score of a Core Channel Node (CCN)"

    print(df)

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
