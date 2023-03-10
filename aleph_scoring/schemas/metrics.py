from typing import List, Optional

from pydantic import BaseModel


class AlephNodeMetrics(BaseModel):
    measured_at: float
    node_id: str
    url: str
    asn: Optional[int]
    as_name: Optional[str]
    version: Optional[str]
    base_latency: Optional[float]


class CcnMetrics(AlephNodeMetrics):
    metrics_latency: Optional[float]
    aggregate_latency: Optional[float]
    file_download_latency: Optional[float]
    txs_total: Optional[int]
    pending_messages: Optional[int]
    eth_height_remaining: Optional[int]


class CrnMetrics(AlephNodeMetrics):
    diagnostic_vm_latency: Optional[float]
    full_check_latency: Optional[float]


class NodeMetrics(BaseModel):
    server: str
    server_asn: int
    server_as_name: str
    ccn: List[CcnMetrics]
    crn: List[CrnMetrics]


class MetricsPost(BaseModel):
    version: str = "1.0"
    tags: List[str]
    # ethereum_height: str
    metrics: NodeMetrics
