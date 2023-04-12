from datetime import datetime
from enum import Enum
from functools import partial
from typing import Optional, Tuple, List

import asyncpg
import requests
from cachetools import TTLCache, cached
from pydantic import BaseModel

from .config import Settings


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
