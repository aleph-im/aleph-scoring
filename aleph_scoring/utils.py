import asyncio
import logging
import re
from datetime import datetime
from enum import Enum
from functools import partial
from random import random
from typing import Any, Callable, Dict, Generator, List, NewType, Optional, Tuple

import aiohttp
import async_timeout
import asyncpg
import requests
from aleph.sdk import AlephClient
from cachetools import TTLCache, cached
from pydantic import BaseModel, validator
from urllib3.util import Url, parse_url

from aleph_scoring.config import settings

from .config import Settings

logger = logging.getLogger(__name__)

TimeoutGenerator = NewType("TimeoutGenerator", Callable[[], aiohttp.ClientTimeout])


class Period(BaseModel):
    from_date: datetime
    to_date: datetime


class LogLevel(int, Enum):
    CRITICAL = 50
    FATAL = CRITICAL
    ERROR = 40
    WARNING = 30
    WARN = WARNING
    INFO = 20
    DEBUG = 10
    NOTSET = 0


class GithubRelease(BaseModel):
    tag_name: str
    name: str
    created_at: datetime
    published_at: datetime
    prerelease: bool


# Cache requests to GitHub to avoid reaching rate limiting.
@cached(cache=TTLCache(maxsize=1, ttl=600))
def get_github_release(owner: str, repository: str, release: str) -> GithubRelease:
    uri = f"https://api.github.com/repos/{owner}/{repository}/releases/{release}"
    response = requests.get(uri)
    response.raise_for_status()

    return GithubRelease.parse_raw(response.text)


def get_latest_release(
    releases_dict: List,
    is_prerelease: bool = False,
    released_before: Optional[GithubRelease] = None,
) -> Optional[GithubRelease]:
    release_before_seen = False  # Indicates wether that version has already been seen.

    for release_dict in releases_dict:
        release: GithubRelease = GithubRelease.parse_obj(release_dict)

        # The releases_dict is sorted by dates in reverse order, the most recent versions appear first.
        # The term 'before' refers to the date, and arrives later in the iteration.
        if released_before:
            if release.tag_name == released_before.tag_name:
                release_before_seen = True
                continue

            # Continue searching since the release should have been released before the `released_before` one.
            if not release_before_seen:
                continue

        if is_prerelease:
            if release.prerelease:
                return release
            else:
                # A prerelease may not appear before a normal release
                return None
        else:
            if release.prerelease:
                continue
            else:
                return release


# Cache requests to GitHub to avoid reaching rate limiting.
@cached(cache=TTLCache(maxsize=1, ttl=600))
def get_latest_github_releases(
    owner: str, repository: str
) -> Tuple[GithubRelease, Optional[GithubRelease], Optional[GithubRelease]]:
    uri = f"https://api.github.com/repos/{owner}/{repository}/releases"
    response = requests.get(uri)
    response.raise_for_status()
    result = response.json()

    latest_release: GithubRelease = get_latest_release(result, is_prerelease=False)
    previous_release: Optional[GithubRelease] = get_latest_release(
        result, released_before=latest_release
    )
    prerelease: Optional[GithubRelease] = get_latest_release(result, is_prerelease=True)

    return latest_release, previous_release, prerelease


get_latest_github_release = partial(get_github_release, release="latest")


async def database_connection(settings: Settings):
    return await asyncpg.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        database=settings.DATABASE_DATABASE,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
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


async def get_aleph_nodes() -> Dict:
    async with AlephClient(api_server=settings.NODE_DATA_HOST) as client:
        return await client.fetch_aggregate(
            address=settings.NODE_DATA_ADDR,
            key="corechannel",
            limit=50,
        )

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
