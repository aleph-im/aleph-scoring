from typing import List

from aleph_message.models import ItemHash
from pydantic import BaseModel, ConstrainedFloat, confloat


class Score(ConstrainedFloat):
    min = 0
    max = 1


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
    base_latency_score_p25: float
    base_latency_score_p95: float
    metrics_latency_score_p25: float
    metrics_latency_score_p95: float
    aggregate_latency_score_p25: float
    aggregate_latency_score_p95: float
    file_download_latency_score_p25: float
    file_download_latency_score_p95: float
    eth_height_remaining_score_p25: float
    eth_height_remaining_score_p95: float


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
