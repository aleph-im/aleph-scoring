from typing import List

from aleph_message.models import ItemHash
from pydantic import BaseModel, confloat

Score = confloat(ge=0, le=1)


class BaseNodeMeasurements(BaseModel):
    total_nodes: int
    nodes_with_identical_asn: int

    base_latency_score_p25: float
    base_latency_score_p95: float

    node_version_latest: int
    node_version_outdated: int
    node_version_obsolete: int
    node_version_missing: int


class CrnMeasurements(BaseNodeMeasurements):
    diagnostic_vm_latency_score_p25: float
    diagnostic_vm_latency_score_p95: float
    full_check_latency_score_p25: float
    full_check_latency_score_p95: float


class CcnMeasurements(BaseNodeMeasurements):
    ...
    # aggregate_latency: Score
    # file_download_latency: Score
    # metrics_endpoint_latency: Score
    # eth_height_remaining: Score
    # pending_messages: Score


class AlephNodeScore(BaseModel):
    node_id: str
    total_score: Score
    performance: Score
    version: Score
    decentralization: Score


class CcnScore(AlephNodeScore):
    measurements: CcnMeasurements


class CrnScore(AlephNodeScore):
    measurements: CrnMeasurements


class NodeScores(BaseModel):
    ccn: List[CcnScore]
    crn: List[CrnScore]


class NodeScoresPost(BaseModel):
    version: str = "1.0"
    tags: List[str]
    metrics_post: ItemHash
    scores: NodeScores
