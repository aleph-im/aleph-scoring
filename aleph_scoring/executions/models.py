from datetime import datetime
from typing import Dict, List, Optional, Dict
from datetime import datetime

from pydantic import BaseModel, Field


class AlephNodeExecutions(BaseModel):
    measured_at: float
    node_id: str
    url: str



class CcnExecutions(AlephNodeExecutions):
    Executions_latency: Optional[float]
    aggregate_latency: Optional[float]
    file_download_latency: Optional[float]
    txs_total: Optional[int]
    pending_messages: Optional[int]
    eth_height_remaining: Optional[int]


class MappedPort(BaseModel):
    host: int
    tcp: bool
    udp: bool


class Networking(BaseModel):
    ipv4_network: str | None
    host_ipv4: str | None
    ipv6_network: str | None
    ipv6_ip: str | None
    ipv4_ip: str | None
    mapped_ports: Dict[str, MappedPort] | None


class ExecutionStatus(BaseModel):
    defined_at: datetime
    preparing_at: Optional[datetime] = None
    prepared_at: Optional[datetime] = None
    starting_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    stopping_at: Optional[datetime] = None
    stopped_at: Optional[datetime] = None


class Execution(BaseModel):
    networking: Networking
    status: ExecutionStatus
    running: bool | None = None


class CrnExecutions(AlephNodeExecutions):
    executions: Dict[str, Execution] | None = Field(
        default=None,
        description="List of executions on the node",
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
    executions: NodeExecutions
