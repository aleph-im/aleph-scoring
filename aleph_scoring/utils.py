import datetime as dt
from functools import partial

import requests
from cachetools import cached, TTLCache

from pydantic import BaseModel


class GithubRelease(BaseModel):
    tag_name: str
    name: str
    created_at: dt.datetime
    published_at: dt.datetime


# Cache requests to GitHub to avoid reaching rate limiting.
@cached(cache=TTLCache(maxsize=1, ttl=600))
def get_github_release(owner: str, repository: str, release: str) -> GithubRelease:
    uri = f"https://api.github.com/repos/{owner}/{repository}/releases/{release}"
    response = requests.get(uri)
    response.raise_for_status()

    return GithubRelease.parse_raw(response.text)


get_latest_github_release = partial(get_github_release, release="latest")
