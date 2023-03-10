import logging
from datetime import datetime
from enum import Enum
from functools import partial
from typing import NamedTuple, Optional, Tuple

import asyncpg
import requests
from cachetools import TTLCache, cached
from pydantic import BaseModel

from .config import Settings

Period = NamedTuple("Period", [("from_date", datetime), ("to_date", datetime)])


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


# Cache requests to GitHub to avoid reaching rate limiting.
@cached(cache=TTLCache(maxsize=1, ttl=600))
def get_github_release(owner: str, repository: str, release: str) -> GithubRelease:
    uri = f"https://api.github.com/repos/{owner}/{repository}/releases/{release}"
    response = requests.get(uri)
    response.raise_for_status()

    return GithubRelease.parse_raw(response.text)


# Cache requests to GitHub to avoid reaching rate limiting.
@cached(cache=TTLCache(maxsize=1, ttl=600))
def get_latest_github_releases(
    owner: str, repository: str
) -> Tuple[GithubRelease, Optional[GithubRelease]]:
    uri = f"https://api.github.com/repos/{owner}/{repository}/releases"
    response = requests.get(uri)
    response.raise_for_status()
    result = response.json()

    latest_release = GithubRelease.parse_obj(result[0])
    previous_release = GithubRelease.parse_obj(result[1]) if len(result) > 1 else None
    return latest_release, previous_release


get_latest_github_release = partial(get_github_release, release="latest")


async def database_connection(settings: Settings):
    return await asyncpg.connect(
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
        database=settings.DATABASE_DATABASE,
        host=settings.DATABASE_HOST,
        port=settings.DATABASE_PORT,
    )
