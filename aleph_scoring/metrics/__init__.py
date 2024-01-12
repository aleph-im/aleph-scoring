import asyncio
import logging
import re
import socket
import subprocess
import time
from datetime import datetime
from ipaddress import IPv6Network, IPv6Address, IPv4Address
from random import shuffle, random
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    NewType,
)
from urllib.parse import urlparse
from icmplib import async_ping
import aiohttp
import async_timeout
import pyasn
from aleph.sdk import AlephClient
from pydantic import BaseModel, validator
from urllib3.util import Url, parse_url

from aleph_scoring.config import settings
from aleph_scoring.metrics.asn import get_asn_database
from aleph_scoring.types.vm_type import VmType
from .models import AlephNodeMetrics, CcnMetrics, CrnMetrics, NodeMetrics

logger = logging.getLogger(__name__)

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

CRN_DIAGNOSTIC_VM_HASH = (
    "67705389842a0a1b95eaa408b009741027964edc805997475e95c505d642edd8"
)

VRF_VM_HASH = (
    "f6a734dbc98659f030e1cd9c12d8ffb769deac55d42d5db5285fba099755c779"
)

CRN_DIAGNOSTIC_VM_PATH = "{url}vm/" + CRN_DIAGNOSTIC_VM_HASH
VRF_VM_PATH = "{url}vm/" + VRF_VM_HASH
IP4_SERVICE_URL = "https://v4.ident.me/"


TimeoutGenerator = NewType("TimeoutGenerator", Callable[[], aiohttp.ClientTimeout])


def timeout_generator(
    total: float, connect: float, sock_connect: float, sock_read: float
) -> TimeoutGenerator:
    def randomize(value: float) -> float:
        return value + value * 0.3 * random()

    return lambda: aiohttp.ClientTimeout(
        total=randomize(total),
        connect=randomize(connect),
        sock_connect=randomize(sock_connect),
        sock_read=randomize(sock_read),
    )


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
    for node in raw_data["nodes"]:
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
    for node in raw_data["resource_nodes"]:
        addr = node["address"].strip("/")
        if addr:
            if not addr.startswith("https://"):
                addr = "https://" + addr
            url: Url = parse_url(addr + "/")
            if url.query:
                logger.warning("Unsupported url for node %s", node["hash"])
            yield NodeInfo(
                url=url,
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
        async with async_timeout.timeout(
            timeout_seconds + timeout_seconds * 0.3 * random()
        ):
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
                    logger.debug(f"Success when fetching {url}")
                    return end - start, output
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


async def get_crn_version(
    session: aiohttp.ClientSession, node_url: str
) -> Optional[str]:
    # Retrieve the CRN version from header `server`.
    try:
        async with async_timeout.timeout(
            settings.HTTP_REQUEST_TIMEOUT
            + settings.HTTP_REQUEST_TIMEOUT * 0.3 * random(),
        ):
            async with session.get(node_url) as resp:
                resp.raise_for_status()
                if "Server" not in resp.headers:
                    return None
                for server in resp.headers.getall("Server"):
                    version: List[str] = re.findall(r"^aleph-vm/(.*)$", server)
                    if version and version[0]:
                        return version[0]
                else:
                    return None

    except (aiohttp.ClientResponseError, aiohttp.ClientConnectorError):
        logger.debug(f"Error when fetching version from {node_url}")
        return None
    except asyncio.TimeoutError:
        logger.debug(f"Timeout error when fetching version from  {node_url}")
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


def get_ipv6(url: str) -> Optional[str]:
    domain = get_url_domain(url)
    try:
        addrinfo = socket.getaddrinfo(domain, None, socket.AF_INET6)
        return addrinfo[0][4][0]
    except socket.gaierror:
        return None


def get_executable_ipv6(
    crn_ipv6_range: IPv6Network, vm_type: VmType, item_hash: str
) -> IPv6Address:
    ipv6_elems = crn_ipv6_range.exploded.split(":")[:4]
    ipv6_elems += [str(vm_type.value)]

    # Add the item hash of the VM as the last 44 bits of the IPv6 address.
    # We expect the VM interface to be set up to use the "1" address of the /124 subnet.
    ipv6_elems += [item_hash[0:4], item_hash[4:8], item_hash[8:11] + "1"]

    return IPv6Address(":".join(ipv6_elems))


async def ping(
    ip_address: Union[IPv4Address, IPv6Address], count: int
) -> Optional[float]:
    result = await async_ping(
        address=str(ip_address), count=count, timeout=2, privileged=False
    )
    if result.is_alive:
        return result.avg_rtt

    logger.debug("Ping %s timed out", str(ip_address))
    return None


async def ping_vm(crn_url: str, vm_hash: str) -> Optional[float]:
    crn_ipv6 = get_ipv6(crn_url)
    if not crn_ipv6:
        return None

    crn_ipv6_range = IPv6Network(crn_ipv6, strict=False)
    vm_ipv6 = get_executable_ipv6(
        crn_ipv6_range=crn_ipv6_range, vm_type=VmType.microvm, item_hash=vm_hash
    )

    average_response_time = await ping(vm_ipv6, count=1)
    if average_response_time:
        logger.debug(
            "VM %s is reachable over IPv6, pinged in %.2f seconds",
            vm_ipv6,
            average_response_time,
        )

    return average_response_time


def lookup_asn(
    asn_db: pyasn.pyasn, url: str
) -> Union[Tuple[str, str], Tuple[None, None]]:
    ip_addr = get_ipv4(url)
    if ip_addr is None:
        logger.debug("Could not determine IP address for %s", url)
        return None, None
    asn = asn_db.lookup(ip_addr)[0]
    if asn is None:
        logger.debug("ASN lookup for (%s) %s did not return a result", ip_addr, url)
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
    timeout_generator: TimeoutGenerator, asn_db: pyasn.pyasn, node_info: NodeInfo
) -> CcnMetrics:
    # Avoid doing all the calls at the same time
    await asyncio.sleep(random() * 30)

    url = node_info.url.url
    measured_at = datetime.utcnow()

    asn, as_name = lookup_asn(asn_db, url)

    # Fetch the base latency using strict IPv4
    async with aiohttp.ClientSession(
        timeout=timeout_generator(),
        connector=aiohttp.TCPConnector(
            family=socket.AF_INET,
            keepalive_timeout=300,
            limit=1000,
            limit_per_host=20,
        ),
    ) as session_ipv4:
        base_latency_ipv4 = (
            await measure_http_latency(session_ipv4, f"{url}api/v0/info/public.json")
        )[0]

    # Fetch most metrics using either IPv4 or IPv6
    async with aiohttp.ClientSession(
        timeout=timeout_generator(),
        connector=aiohttp.TCPConnector(
            family=0,  # either IPv4 or IPv6
            keepalive_timeout=300,
            limit=1000,
            limit_per_host=20,
        ),
    ) as session:
        # Fetch base latency again in order to pre-open the session
        _ = (await measure_http_latency(session, f"{url}api/v0/info/public.json"))[0]
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
            session,
            f"{url}metrics.json",
            settings.HTTP_REQUEST_TIMEOUT,
            return_output=True,
        )

        if json_text is not None:
            json_object = CcnApiMetricsResponse.parse_obj(json_text)
        else:
            json_object = CcnApiMetricsResponse()
        version = json_object.version()

    # Fetch the base latency using strict IPv6
    # async with aiohttp.ClientSession(
    #     timeout=timeout_generator(),
    #     connector=aiohttp.TCPConnector(
    #         family=socket.AF_INET6,
    #         keepalive_timeout=300,
    #         limit=1000,
    #         limit_per_host=20,
    #     ),
    # ) as session_ipv6:
    #     _ = (await measure_http_latency(session_ipv6, f"{url}api/v0/info/public.json"))[0]
    #     base_latency_ipv6 = (
    #         await measure_http_latency(session_ipv6, f"{url}api/v0/info/public.json")
    #     )[0]

    # There is currently no IPv6 in the multiaddr of CCNs
    base_latency_ipv6 = None

    return CcnMetrics(
        measured_at=measured_at.timestamp(),
        node_id=node_info.hash,
        url=url,
        asn=asn,
        as_name=as_name,
        version=version,
        # days_outdated=compute_ccn_version_days_outdated(version=version),
        base_latency=base_latency_ipv6
        or base_latency_ipv4,  # allow either IPv6 or IPv4 for now
        base_latency_ipv4=base_latency_ipv4,
        metrics_latency=metrics_latency,
        aggregate_latency=aggregate_latency,
        file_download_latency=file_download_latency,
        txs_total=json_object.pyaleph_status_sync_pending_txs_total,
        pending_messages=json_object.pyaleph_status_sync_pending_messages_total,  # noqa:E501
        eth_height_remaining=json_object.pyaleph_status_chain_eth_height_remaining_total,
    )


async def get_crn_metrics(
    timeout_generator: TimeoutGenerator, asn_db: pyasn.pyasn, node_info: NodeInfo
) -> CrnMetrics:
    # Avoid doing all the calls at the same time
    await asyncio.sleep(random() * 30)

    url = node_info.url.url
    measured_at = datetime.utcnow()

    asn, as_name = lookup_asn(asn_db, url)

    # Get the version over IPv4 or IPv6
    async with aiohttp.ClientSession(timeout=timeout_generator()) as session_any_ip:
        for attempt in range(3):
            version = await get_crn_version(session=session_any_ip, node_url=url)
            if version:
                break

    async with aiohttp.ClientSession(
        timeout=timeout_generator(),
        connector=aiohttp.TCPConnector(
            family=socket.AF_INET6,
            keepalive_timeout=300,
            limit=1000,
            limit_per_host=20,
        ),
    ) as session:
        # Warmup the session
        _ = await get_crn_version(session=session, node_url=url)

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

        if diagnostic_vm_latency is not None:
            vm_ping_latency = await ping_vm(
                crn_url=node_info.url.url, vm_hash=CRN_DIAGNOSTIC_VM_HASH
            )
        else:
            logger.debug("Could not start diagnostic VM, skipping IPv6 ping check")
            vm_ping_latency = None

        full_check_latency = (
            await measure_http_latency(
                session,
                f"{url}status/check/fastapi",
                timeout_seconds=20,
            )
        )[0]

        vrf_vm_latency = (
            await measure_http_latency(
                session,
                "".join(VRF_VM_PATH).format(url=url),
                timeout_seconds=10,
            )
        )[0]

    async with aiohttp.ClientSession(
        timeout=timeout_generator(),
        connector=aiohttp.TCPConnector(
            family=socket.AF_INET,
            keepalive_timeout=300,
            limit=1000,
            limit_per_host=20,
        ),
    ) as session_ipv4:
        # Warmup the session
        _ = await get_crn_version(session=session_ipv4, node_url=url)

        base_latency_ipv4 = (
            await measure_http_latency(
                session_ipv4,
                f"{url}about/login",
                expected_status=401,
            )
        )[0]

    return CrnMetrics(
        measured_at=measured_at.timestamp(),
        node_id=node_info.hash,
        url=url,
        asn=asn,
        as_name=as_name,
        version=version,
        # days_outdated=compute_crn_version_days_outdated(version=version),
        base_latency=base_latency,
        base_latency_ipv4=base_latency_ipv4,
        diagnostic_vm_latency=diagnostic_vm_latency,
        full_check_latency=full_check_latency,
        vm_ping_latency=vm_ping_latency,
        vrf_latency=vrf_vm_latency,
    )


M = TypeVar("M", bound=AlephNodeMetrics)


async def collect_node_metrics(
    node_infos: Iterable[NodeInfo],
    metrics_function: Callable[[TimeoutGenerator, pyasn.pyasn, NodeInfo], Awaitable[M]],
) -> Sequence[Union[M, BaseException]]:
    asn_db = get_asn_database()
    timeout = timeout_generator(
        total=60.0, connect=10.0, sock_connect=10.0, sock_read=60.0
    )
    return await asyncio.gather(
        *[metrics_function(timeout, asn_db, node_info) for node_info in node_infos]
    )


async def collect_all_ccn_metrics(node_data: Dict[str, Any]) -> Sequence[CcnMetrics]:
    node_infos = list(get_api_node_urls(node_data))
    shuffle(node_infos)  # Avoid artifacts from the order in the list
    return await collect_node_metrics(
        node_infos=node_infos, metrics_function=get_ccn_metrics
    )


async def collect_all_crn_metrics(node_data: Dict[str, Any]) -> Sequence[CrnMetrics]:
    node_infos = list(get_compute_resource_node_urls(node_data))
    shuffle(node_infos)  # Avoid artifacts from the order in the list
    return await collect_node_metrics(
        node_infos=node_infos, metrics_function=get_crn_metrics
    )


async def get_aleph_nodes() -> Dict:
    async with AlephClient(api_server=settings.NODE_DATA_HOST) as client:
        return await client.fetch_aggregate(
            address=settings.NODE_DATA_ADDR,
            key="corechannel",
            limit=50,
        )


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
    logger.debug("Fetched node data")
    ccn_metrics = await collect_all_ccn_metrics(aleph_nodes)
    logger.debug("Fetched CCN metrics")
    crn_metrics = await collect_all_crn_metrics(aleph_nodes)
    logger.debug("Fetched CRN metrics")

    return NodeMetrics(
        server=ip_address,
        server_asn=asn,
        server_as_name=as_name,
        ccn=ccn_metrics,
        crn=crn_metrics,
    )


async def measure_node_performance() -> NodeMetrics:
    logger.debug("Measuring node performance")
    node_metrics = await collect_all_node_metrics()
    return node_metrics


def measure_node_performance_sync() -> NodeMetrics:
    return asyncio.run(measure_node_performance())
