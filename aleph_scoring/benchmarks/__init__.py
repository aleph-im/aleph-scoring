import asyncio
import logging
import socket
from datetime import datetime
from random import random, shuffle
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    NewType,
    Sequence,
    TypeVar,
    Union,
)

import aiohttp
from aleph_scoring.config import settings
from aleph_scoring.utils import (
    NodeInfo,
    get_aleph_nodes,
    get_compute_resource_node_urls,
    get_crn_version,
    timeout_generator,
)

from .models import CrnBenchmarks, NodeBenchmarks

logger = logging.getLogger(__name__)

TimeoutGenerator = NewType("TimeoutGenerator", Callable[[], aiohttp.ClientTimeout])

B = TypeVar("B")


async def get_crn_benchmarks(
    timeout_generator: TimeoutGenerator, node_info: NodeInfo
) -> CrnBenchmarks:
    # Avoid doing all the calls at the same time
    await asyncio.sleep(random() * 30)

    url = node_info.url.url
    measured_at = datetime.utcnow()

    # Get the version over IPv4 or IPv6
    async with aiohttp.ClientSession(timeout=timeout_generator()) as session_any_ip:
        for attempt in range(3):
            version = await get_crn_version(session=session_any_ip, node_url=url)
            if version:
                break

    async with aiohttp.ClientSession(
        timeout=timeout_generator(),
        connector=aiohttp.TCPConnector(
            family=socket.AF_INET,
            keepalive_timeout=300,
            limit=1000,
            limit_per_host=20,
        ),
    ) as session_ipv4:
        try:
            async with session_ipv4.get(f"{url}vm/{settings.BENCHMARK_VM_HASH}/sysbench/cpu") as resp:
                if resp.status != 200:
                    cpu_bench = None
                else:
                    cpu_bench = await resp.json()

            async with session_ipv4.get(f"{url}vm/{settings.BENCHMARK_VM_HASH}/sysbench/memory") as resp:
                if resp.status != 200:
                    ram_bench = None
                else:
                    ram_bench = await resp.json()
            async with session_ipv4.get(f"{url}vm/{settings.BENCHMARK_VM_HASH}/sysbench/disk") as resp:
                if resp.status != 200:
                    disk_bench = None
                else:
                    disk_bench = await resp.json()
        except aiohttp.ClientResponseError:
            logger.debug(f"Error when fetching {url}")
            cpu_bench = None
            ram_bench = None
            disk_bench = None
        except aiohttp.ClientConnectorError:
            logger.debug(f"Error when fetching {url}")
            cpu_bench = None
            ram_bench = None
            disk_bench = None
        except asyncio.TimeoutError:
            logger.debug(f"Timeout error when fetching {url}")
            cpu_bench = None
            ram_bench = None
            disk_bench = None

        return CrnBenchmarks(
            measured_at=measured_at.timestamp(),
            node_id=node_info.hash,
            version=version,
            cpu=cpu_bench,
            ram=ram_bench,
            disk=disk_bench
        )


async def collect_all_crn_benchmarks(node_data: Dict[str, Any]) -> Sequence[CrnBenchmarks]:
    node_infos = list(get_compute_resource_node_urls(node_data))
    shuffle(node_infos)  # Avoid artifacts from the order in the list
    return await collect_node_benchmarks(
        node_infos=node_infos, metrics_function=get_crn_benchmarks
    )


async def collect_node_benchmarks(
    node_infos: Iterable[NodeInfo],
    metrics_function: Callable[[TimeoutGenerator, NodeInfo], Awaitable[B]],
) -> Sequence[Union[B, BaseException]]:
    timeout = timeout_generator(
        total=60.0, connect=10.0, sock_connect=10.0, sock_read=60.0
    )
    return await asyncio.gather(
        *[metrics_function(timeout, node_info) for node_info in node_infos]
    )


async def benchmark_node_performance() -> NodeBenchmarks:
    logger.debug("Benchmark node performance")
    aleph_nodes = await get_aleph_nodes()
    crn_benchmarks = await collect_all_crn_benchmarks(aleph_nodes)
    return NodeBenchmarks(
        crn=crn_benchmarks
    )


def benchmark_node_performance_sync() -> NodeBenchmarks:
    return asyncio.run(benchmark_node_performance())
