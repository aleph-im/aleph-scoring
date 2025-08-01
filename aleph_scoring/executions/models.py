from typing import List, Optional

from pydantic import BaseModel, Field


class AlephNodeExecutions(BaseModel):
    measured_at: float
    node_id: str
    url: str
    # asn: Optional[int]
    # as_name: Optional[str]
    # version: Optional[str]
    # days_outdated: Optional[int] = None  # TODO
    # base_latency: Optional[float]
    # base_latency_ipv4: Optional[float]


class CcnExecutions(AlephNodeExecutions):
    Executions_latency: Optional[float]
    aggregate_latency: Optional[float]
    file_download_latency: Optional[float]
    txs_total: Optional[int]
    pending_messages: Optional[int]
    eth_height_remaining: Optional[int]


class CrnExecutions(AlephNodeExecutions):
    executions: dict| None = Field(
        default_factory=list, description="List of executions on the node"
    )


class NodeExecutions(BaseModel):
    server: str
    # server_asn: int
    # server_as_name: str
    # ccn: List[CcnExecutions]
    crn: List[CrnExecutions] = Field(
        default_factory=list, description="List of executions per node"
    )


class ExecutionsPost(BaseModel):
    version: str = "1.0"
    tags: List[str]
    # ethereum_height: str
    executions: NodeExecutions
