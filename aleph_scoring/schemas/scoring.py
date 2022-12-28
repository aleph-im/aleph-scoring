from typing import List

from aleph_message.models import ItemHash
from pydantic import BaseModel, confloat

Score = confloat(ge=0, le=1)


class AlephNodeScore(BaseModel):
    node_id: str
    total_score: Score
    version: Score
    base_latency: Score
    decentralization: Score


class CcnScore(AlephNodeScore):
    aggregate_latency: Score
    file_download_latency: Score
    metrics_endpoint_latency: Score
    eth_height_remaining: Score
    pending_messages: Score


class CrnScore(AlephNodeScore):
    diagnostic_vm_latency: Score
    full_check_latency: Score


class NodeScores(BaseModel):
    ccn: List[CcnScore]
    crn: List[CrnScore]


class NodeScoresPost(BaseModel):
    version: str = "1.0"
    tags: List[str]
    metrics_post: ItemHash
    scores: NodeScores
