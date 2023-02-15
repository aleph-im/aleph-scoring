import asyncio
import logging
import re
import socket
import time
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    Dict,
    Generator,
    Literal,
    Optional,
    Tuple,
    Callable,
    Iterable,
    List,
    Union,
    TypeVar,
    Awaitable,
    Sequence,
)
from urllib.parse import urlparse

import aiohttp
import async_timeout
import pandas as pd
import pyasn
from pydantic import BaseModel, validator
from urllib3.util import Url, parse_url

from .asn import get_asn_database
from .config import settings
from .schemas.metrics import CcnMetrics, CrnMetrics, AlephNodeMetrics, NodeMetrics

LOGGER = logging.getLogger(__name__)

# Global variable used to aggregate the metrics over time
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
IP4_SERVICE_URL = "https://v4.ident.me/"


class NodeInfo(BaseModel):
    url: Url
    hash: str

    @validator("hash")
    def hash_format(cls, v) -> str:
        if len(v) != 64:
            raise ValueError("must have a length of 64")
        try:
            # Parse as hexadecimal using int()
            int(v, 16)
        except ValueError:
            raise ValueError("must be hexadecimal")
        return v


def get_api_node_urls(raw_data: Dict[str, Any]) -> Generator[NodeInfo, None, None]:
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
    return_output: bool = False,
    return_json: bool = True,
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
                if return_output:
                    if return_json:
                        output = await resp.json()
                    else:
                        output = await resp.text()
                    end = time.time()
                    LOGGER.debug(f"Success when fetching {url}")
                    return end - start, output
                else:
                    await resp.release()
                    end = time.time()
                    LOGGER.debug(f"Success when fetching {url}")
                    return end - start, None
    except aiohttp.ClientResponseError:
        LOGGER.debug(f"Error when fetching {url}")
        return None, None
    except aiohttp.ClientConnectorError:
        LOGGER.debug(f"Error when fetching {url}")
        return None, None
    except asyncio.TimeoutError:
        LOGGER.debug(f"Timeout error when fetching {url}")
        return None, None


async def get_crn_version(
    session: aiohttp.ClientSession, node_url: str
) -> Optional[str]:
    # Retrieve the CRN version from header `server`.
    try:
        async with async_timeout.timeout(
            settings.HTTP_REQUEST_TIMEOUT,
        ):
            async with session.get(node_url) as resp:
                resp.raise_for_status()
                print(resp.headers)
                for server in resp.headers.getall("Server"):
                    print("VERSION", [node_url, server])
                    version: List[str] = re.findall(r"^aleph-vm/(.*)$", server)
                    if version and version[0]:
                        return version[0]
                else:
                    return None
    except (aiohttp.ClientResponseError, aiohttp.ClientConnectorError):
        LOGGER.debug(f"Error when fetching version from {node_url}")
        return None
    except asyncio.TimeoutError:
        LOGGER.debug(f"Timeout error when fetching version from  {node_url}")
        return None


def get_url_domain(url: str) -> str:
    domain = urlparse(url).netloc
    return domain.split(":")[0]  # Remove port


def get_ipv4(url: str) -> Optional[str]:
    domain = get_url_domain(url)
    try:
        return socket.gethostbyname(domain)
    except socket.gaierror:
        return None


def lookup_asn(
    asn_db: pyasn.pyasn, url: str
) -> Union[Tuple[str, str], Tuple[None, None]]:
    ip_addr = get_ipv4(url)
    if ip_addr is None:
        LOGGER.debug("Could not determine IP address for %s", url)
        return None, None
    asn = asn_db.lookup(ip_addr)[0]
    if asn is None:
        LOGGER.debug("ASN lookup for (%s) %s did not return a result", ip_addr, url)
        return None, None

    return asn, asn_db.get_as_name(asn)


class CcnBuildInfo(BaseModel):
    python_version: str
    version: str


# Pydantic class to parse json to object
class CcnApiMetricsResponse(BaseModel):
    pyaleph_build_info: Optional[CcnBuildInfo] = None
    pyaleph_status_sync_pending_txs_total: Optional[int] = None
    pyaleph_status_sync_pending_messages_total: Optional[int] = None
    pyaleph_status_chain_eth_height_remaining_total: Optional[int] = None

    class Config:
        allow_population_by_field_name = True

    def version(self) -> Optional[str]:
        if self.pyaleph_build_info:
            return self.pyaleph_build_info.version
        return None


async def get_ccn_metrics(
    session: aiohttp.ClientSession, asn_db: pyasn.pyasn, node_info: NodeInfo
) -> CcnMetrics:
    url = node_info.url.url
    measured_at = datetime.utcnow()

    asn, as_name = lookup_asn(asn_db, url)

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
        session, f"{url}metrics.json", settings.HTTP_REQUEST_TIMEOUT, return_output=True
    )

    if json_text is not None:
        json_object = CcnApiMetricsResponse(**json_text)
    else:
        json_object = CcnApiMetricsResponse()

    return CcnMetrics(
        measured_at=measured_at.timestamp(),
        node_id=node_info.hash,
        url=url,
        asn=asn,
        as_name=as_name,
        version=json_object.version(),
        base_latency=base_latency,
        metrics_latency=metrics_latency,
        aggregate_latency=aggregate_latency,
        file_download_latency=file_download_latency,
        txs_total=json_object.pyaleph_status_sync_pending_txs_total,
        pending_messages=json_object.pyaleph_status_sync_pending_messages_total,  # noqa:E501
        eth_height_remaining=json_object.pyaleph_status_chain_eth_height_remaining_total,
    )


async def get_crn_metrics(
    session: aiohttp.ClientSession, asn_db: pyasn.pyasn, node_info: NodeInfo
) -> CrnMetrics:
    url = node_info.url.url
    measured_at = datetime.utcnow()

    asn, as_name = lookup_asn(asn_db, url)

    version = await get_crn_version(session=session, node_url=url)

    base_latency = (
        await measure_http_latency(
            session,
            f"{url}about/login",
            expected_status=401,
        )
    )[0]

    diagnostic_vm_latency = (
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

    return CrnMetrics(
        measured_at=measured_at.timestamp(),
        node_id=node_info.hash,
        url=url,
        asn=asn,
        as_name=as_name,
        version=version,
        base_latency=base_latency,
        diagnostic_vm_latency=diagnostic_vm_latency,
        full_check_latency=full_check_latency,
    )


M = TypeVar("M", bound=AlephNodeMetrics)


async def collect_node_metrics(
    node_infos: Iterable[NodeInfo],
    metrics_function: Callable[
        [aiohttp.ClientSession, pyasn.pyasn, NodeInfo], Awaitable[M]
    ],
) -> Sequence[Union[M, BaseException]]:
    asn_db = get_asn_database()
    timeout = aiohttp.ClientTimeout(
        total=60.0, connect=2.0, sock_connect=2.0, sock_read=60.0
    )
    async with aiohttp.ClientSession(timeout=timeout) as session:
        return await asyncio.gather(
            *[metrics_function(session, asn_db, node_info) for node_info in node_infos]
        )


async def collect_all_ccn_metrics(node_data: Dict[str, Any]) -> Sequence[CcnMetrics]:
    node_infos = get_api_node_urls(node_data)
    return await collect_node_metrics(
        node_infos=node_infos, metrics_function=get_ccn_metrics
    )


async def collect_all_crn_metrics(node_data: Dict[str, Any]) -> Sequence[CrnMetrics]:
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


async def collect_server_metadata(asn_db: pyasn.pyasn) -> Tuple[str, int, str]:
    def is_valid_ip4(ip: str) -> bool:
        return bool(re.match(r"\d+\.\d+\.\d+\.\d+", ip))

    async def get_ip4_from_service() -> str:
        """Get the public IPv4 of this system by calling a third-party service"""
        async with aiohttp.ClientSession() as session:
            async with session.get(IP4_SERVICE_URL) as resp:
                resp.raise_for_status()
                ip = await resp.text()

                if is_valid_ip4(ip):
                    return ip
                else:
                    raise ValueError(f"Response does not match IPv4 format: {ip}")

    ip_address = await get_ip4_from_service()
    # lookup_asn does not work for localhost
    asn = asn_db.lookup(ip_address)[0]
    as_name = asn_db.get_as_name(asn)

    return ip_address, asn, as_name


async def collect_all_node_metrics() -> NodeMetrics:
    # Scoring server info
    ip_address, asn, as_name = await collect_server_metadata(get_asn_database())

    # Aleph node metrics
    aleph_nodes = await get_aleph_nodes()
    LOGGER.debug("Fetched node data")
    ccn_metrics = await collect_all_ccn_metrics(aleph_nodes)
    LOGGER.debug("Fetched CCN metrics")
    crn_metrics = await collect_all_crn_metrics(aleph_nodes)
    LOGGER.debug("Fetched CRN metrics")

    return NodeMetrics(
        server=ip_address,
        server_asn=asn,
        server_as_name=as_name,
        ccn=ccn_metrics,
        crn=crn_metrics,
    )


async def measure_node_performance() -> NodeMetrics:
    LOGGER.debug("Measuring node performance")
    node_metrics = await collect_all_node_metrics()
    return node_metrics


def measure_node_performance_sync() -> NodeMetrics:
    return asyncio.run(measure_node_performance())
