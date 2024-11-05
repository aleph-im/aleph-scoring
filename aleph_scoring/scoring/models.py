from typing import List

from pydantic import BaseModel, ConstrainedFloat

from aleph_scoring.utils import Period


class Score(ConstrainedFloat):
    min = 0
    max = 1


class BaseNodeMeasurements(BaseModel):
    total_nodes: int
    nodes_with_identical_asn: int

    record_count: int
    total_score: Score


class CrnMeasurements(BaseNodeMeasurements):
    pass


class CcnMeasurements(BaseNodeMeasurements):
    pass


class AlephNodeScore(BaseModel):
    node_id: str
    total_score: Score
    decentralization: Score


class CcnScore(AlephNodeScore):
    measurements: CcnMeasurements


class CrnScore(AlephNodeScore):
    measurements: CrnMeasurements


class NodeScores(BaseModel):
    ccn: List[CcnScore]
    crn: List[CrnScore]


class NodeScoresPost(BaseModel):
    version: str = "1.1"
    tags: List[str]
    period: Period
    scores: NodeScores
