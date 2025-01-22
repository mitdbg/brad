import enum
from typing import List
from pydantic import BaseModel

from brad.config.engine import Engine

# This is a simple implementation of a Virtual Database Engine (VDBE) metadata
# model meant for demonstration purposes only.


class VirtualTable(BaseModel):
    name: str
    writable: bool


class SchemaTable(BaseModel):
    name: str


class QueryInterface(enum.Enum):
    Common = "common"
    PostgreSQL = "postgresql"
    Athena = "athena"


class VirtualEngine(BaseModel):
    name: str
    max_staleness_ms: int
    p90_latency_slo_ms: int
    interface: QueryInterface
    tables: List[VirtualTable]
    mapped_to: Engine


class VirtualInfrastructure(BaseModel):
    schema_name: str
    engines: List[VirtualEngine]
    tables: List[SchemaTable]
