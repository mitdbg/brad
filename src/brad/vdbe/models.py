import enum
from functools import cached_property
from typing import List, Optional, Set
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
    internal_id: int
    name: str
    max_staleness_ms: int
    p90_latency_slo_ms: int
    interface: QueryInterface
    tables: List[VirtualTable]
    mapped_to: Engine
    endpoint: Optional[str] = None

    @cached_property
    def table_names_set(self) -> Set[str]:
        return {table.name for table in self.tables}


class VirtualInfrastructure(BaseModel):
    schema_name: str
    engines: List[VirtualEngine]
    tables: List[SchemaTable]


class CreateVirtualEngineArgs(BaseModel):
    name: str
    max_staleness_ms: int
    p90_latency_slo_ms: int
    interface: QueryInterface
    tables: List[VirtualTable]
    mapped_to: Engine
