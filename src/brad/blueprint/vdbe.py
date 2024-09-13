import enum

from pydantic import BaseModel
from typing import Set

# This class contains Python definitions


class QueryInterface(enum.Enum):
    SqlPostgresql = "SQL_POSTGRESQL"
    SqlMysql = "SQL_MYSQL"
    SqlAwsRedshift = "AWS_REDSHIFT"
    SqlAwsAthena = "AWS_ATHENA"


class VirtualTable(BaseModel):
    name: str
    writable: bool


class VirtualEngine(BaseModel):
    name: str
    query_interface: QueryInterface
    max_staleness_ms: int
    tables: Set[VirtualTable]
