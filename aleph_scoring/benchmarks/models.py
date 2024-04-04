from typing import List, Optional

from pydantic import BaseModel


class CrnBenchmarks(BaseModel):
    measured_at: float
    node_id: str
    version: Optional[str]
    cpu: Optional[dict]
    ram: Optional[dict]
    disk: Optional[dict]


class NodeBenchmarks(BaseModel):
    crn: List[CrnBenchmarks]


class BenchmarksPost(BaseModel):
    version: str = "1.0"
    tags: List[str]
    benchmarks: NodeBenchmarks
