import asyncio
import logging
import re
import time
from datetime import datetime, timezone
from random import random, shuffle
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generator,
    Iterable,
    Literal,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

import aiohttp
from aleph.sdk import AlephHttpClient
from pydantic import BaseModel, validator
from urllib3.util import Url, parse_url

from aleph_scoring.config import settings
from aleph_scoring.executions.models import (
    AlephNodeExecutions,
    CrnExecutions,
    NodeExecutions,
)

logger = logging.getLogger(__name__)


def seconds_since_process_has_started() -> float:
    """Returns the number of seconds since the process has started."""
    import psutil

    process = psutil.Process()
    return time.time() - process.create_time()


# Remove known test and diagnostic VMs
VM_TO_IGNORES = [
    # Test hash VMs
    "fake_vm_fake_vm_fake_vm_fake_vm_fake_vm_fake_vm_fake_vm_fake_vm_",
    "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe",
    "decadecadecadecadecadecadecadecadecadecadecadecadecadecadecadeca",
    # Diagnostic VM
    "63faf8b5db1cf8d965e6a464a0cb8062af8e7df131729e48738342d956f29ace",
    "67705389842a0a1b95eaa408b009741027964edc805997475e95c505d642edd8",
]
# Global variable used to aggregate the executions over time
ExecutionsLogKey = Literal["core_channel_nodes", "compute_resource_nodes"]

CCN_AGGREGATE_PATH = (
    "{url}api/v0/aggregates/0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10.json"
    "?keys=corechannel"
)


IP4_SERVICE_URLS = ["https://v4.ident.me/", "https://api.ipify.org/"]


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


def get_compute_resource_node_urls(
    raw_data: Dict[str, Any],
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


async def fetch_crn_executions(
    session: aiohttp.ClientSession, node_url: str
) -> Optional[dict[str, Any]]:
    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with asyncio.timeout(settings.HTTP_REQUEST_TIMEOUT):
                async with session.get(node_url) as resp:
                    r = await resp.json()
                    return r

        except aiohttp.ClientError as e:
            if attempt < max_retries - 1:
                wait = 2**attempt
                logger.debug(
                    f"Error when fetching executions from {node_url} ({e}), "
                    f"retrying in {wait}s (attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(wait)
            else:
                logger.warning(
                    f"Failed to fetch executions from {node_url} after {max_retries} attempts: {e}"
                )
                return None
        except asyncio.TimeoutError:
            if attempt < max_retries - 1:
                wait = 2**attempt
                logger.debug(
                    f"Timeout when fetching executions from {node_url}, "
                    f"retrying in {wait}s (attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(wait)
            else:
                logger.warning(
                    f"Timeout fetching executions from {node_url} after {max_retries} attempts"
                )
                return None
    return None


async def get_crn_executions(
    timeout: aiohttp.ClientTimeout, node_info: NodeInfo
) -> CrnExecutions:
    """Fetch or measure the executions of a Core Channel Node."""
    # Starting to call all the nodes at the same time can cause issues, in particular:
    #  - bias due to the computing overhead
    #  - network issues due to opening many concurrent connections
    #  - throttling by hosting providers
    #
    # In order to avoid this scenario, each coroutine (specific to one host)
    # waits for a random time (linear distribution) between 0 and 60 minutes.
    # (excluding the time to get to this step: update ASN database and fetch node list)
    margin_to_publish: float = (
        120  # Allow time to publish the data and not offset the next measurements
    )
    delay_seconds: float = (
        (random() * 60 * 60) - seconds_since_process_has_started() - margin_to_publish
    )
    logger.debug(
        f"Waiting {delay_seconds} seconds before fetching executions for {node_info.hash}"
    )
    # await asyncio.sleep(delay_seconds)
    logger.debug(f"Done waiting for {node_info.hash}")

    url = node_info.url.url
    measured_at = datetime.now(tz=timezone.utc)

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=aiohttp.TCPConnector(
            # family=socket.AF_INET6,
            keepalive_timeout=300,
            limit=1000,
            limit_per_host=20,
        ),
    ) as session:
        executions = await fetch_crn_executions(
            session=session, node_url=url + "v2/about/executions/list"
        )

        filtered_executions: None | dict

        if executions is not None:
            filtered_executions = {}
            for execution_hash, execution in executions.items():
                if execution_hash not in VM_TO_IGNORES:
                    filtered_executions[execution_hash] = execution
        else:
            filtered_executions = executions

    return CrnExecutions(
        measured_at=measured_at.timestamp(),
        node_id=node_info.hash,
        url=url,
        executions=filtered_executions,
    )


M = TypeVar("M", bound=AlephNodeExecutions)


async def collect_node_executions(
    node_infos: Iterable[NodeInfo],
    executions_function: Callable[[aiohttp.ClientTimeout, NodeInfo], Awaitable[M]],
) -> Sequence[Union[M, BaseException]]:
    timeout = aiohttp.ClientTimeout(
        total=60.0, connect=10.0, sock_connect=10.0, sock_read=60.0
    )
    return await asyncio.gather(
        *[executions_function(timeout, node_info) for node_info in node_infos],
        return_exceptions=True,
    )


async def collect_all_crn_executions(
    node_data: Dict[str, Any],
) -> Sequence[CrnExecutions | BaseException]:
    node_infos = list(get_compute_resource_node_urls(node_data))
    # Uncomment during testing to run faster with less node
    # node_infos = [node for node in node_infos if "leviathan" in node.url.url]
    # node_infos = node_infos[:10]
    shuffle(node_infos)  # Avoid artifacts from the order in the list
    return await collect_node_executions(
        node_infos=node_infos, executions_function=get_crn_executions
    )


async def get_aleph_nodes() -> Dict:
    async with AlephHttpClient(api_server=settings.NODE_DATA_HOST) as client:
        return await client.fetch_aggregate(
            address=settings.NODE_DATA_ADDR,
            key="corechannel",
        )


def is_valid_ip4(ip: str) -> bool:
    return bool(re.match(r"\d+\.\d+\.\d+\.\d+", ip))


async def get_ip4_from_service() -> str:
    """Get the public IPv4 of this system by calling a third-party service"""
    for ip4_service_url in IP4_SERVICE_URLS:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(ip4_service_url) as resp:
                    resp.raise_for_status()
                    ip = await resp.text()

                    if is_valid_ip4(ip):
                        return ip
                    else:
                        raise ValueError(f"Response does not match IPv4 format: {ip}")
        except aiohttp.ClientConnectorError as error:
            logger.warning(f"Could not connect to {ip4_service_url}: {error}")
            continue
    else:
        raise ValueError("Could not determine public IPv4 address")


async def collect_all_node_executions() -> NodeExecutions:
    # Scoring server info
    ip_address = await get_ip4_from_service()

    # Aleph node executions
    aleph_nodes = await get_aleph_nodes()
    logger.debug("Fetched node data")

    # CRN and CRN executions are measured concurrently since they are randomly
    # scheduled over the next hour usinc `asyncio.sleep` in each node coroutine.
    (crn_executions,) = await asyncio.gather(
        collect_all_crn_executions(aleph_nodes),
    )

    logger.debug("Fetched node executions")

    return NodeExecutions(
        server=ip_address,
        crn=list(m for m in crn_executions if isinstance(m, CrnExecutions)),
    )


async def record_node_executions() -> NodeExecutions:
    logger.debug("Measuring node performance")
    node_executions = await collect_all_node_executions()
    return node_executions


def record_node_executions_sync() -> NodeExecutions:
    return asyncio.run(record_node_executions())
