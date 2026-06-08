from typing import List, Optional

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
    # IP stability inputs. Defaults are non-penalizing so a node with no
    # recorded IP history yet is never zeroed during the bootstrap period.
    has_ipv4: bool = True
    has_ipv6: bool = True
    ipv4_changes: int = 0
    ipv6_changes: int = 0  # counted at the /64 prefix, not the full address
    ip_penalized: bool = False


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
    measured_at: Optional[str] = None  # ISO formatted datetime
