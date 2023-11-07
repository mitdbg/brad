from typing import List
from datetime import timedelta

from brad.blueprint import Blueprint
from brad.blueprint.provisioning import Provisioning
from brad.blueprint.table import Table
from brad.config.engine import Engine
from brad.planner.neighborhood.filters.aurora_transactions import AuroraTransactions
from brad.planner.neighborhood.filters.no_data_loss import NoDataLoss
from brad.planner.neighborhood.filters.single_engine_execution import (
    SingleEngineExecution,
)
from brad.planner.neighborhood.filters.table_on_engine import TableOnEngine
from brad.planner.workload import Workload
from brad.planner.workload.query import Query
from brad.routing.abstract_policy import FullRoutingPolicy
from brad.routing.always_one import AlwaysOneRouter


def workload_from_queries(query_list: List[str]) -> Workload:
    analytical = []
    transactional = []
    for q in query_list:
        qr = Query(q)
        if qr.is_data_modification_query():
            transactional.append(qr)
        else:
            analytical.append(qr)
    return Workload(timedelta(hours=1), analytical, transactional, {})


def test_aurora_transactions():
    workload1 = workload_from_queries(["INSERT INTO test SELECT * FROM test2"])
    workload2 = workload_from_queries(["SELECT * FROM test", "SELECT * FROM test2"])
    bp1 = Blueprint(
        "schema",
        table_schemas=[
            Table("test", [], [], None, []),
            Table("test2", [], [], None, []),
        ],
        table_locations={
            "test": [Engine.Aurora],
            "test2": [Engine.Redshift, Engine.Athena],
        },
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 1),
        full_routing_policy=FullRoutingPolicy([], AlwaysOneRouter(Engine.Aurora)),
    )
    bp2 = Blueprint(
        "schema",
        table_schemas=[
            Table("test", [], [], None, []),
            Table("test2", [], [], None, []),
        ],
        table_locations={
            "test": [Engine.Aurora],
            "test2": [Engine.Redshift, Engine.Athena, Engine.Aurora],
        },
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 1),
        full_routing_policy=FullRoutingPolicy([], AlwaysOneRouter(Engine.Aurora)),
    )

    bp_filter1 = AuroraTransactions(workload1)
    assert not bp_filter1.is_valid(bp1)
    assert bp_filter1.is_valid(bp2)

    bp_filter2 = AuroraTransactions(workload2)
    assert bp_filter2.is_valid(bp1)
    assert bp_filter2.is_valid(bp2)


def test_single_engine_execution():
    workload1 = workload_from_queries(
        ["SELECT * FROM test, test2", "SELECT * FROM test2, test3"]
    )
    workload2 = workload_from_queries(
        ["SELECT * FROM test", "SELECT * FROM test2", "SELECT * FROM test3"]
    )

    bp1 = Blueprint(
        "schema",
        table_schemas=[
            Table("test", [], [], None, []),
            Table("test2", [], [], None, []),
            Table("test3", [], [], None, []),
        ],
        table_locations={
            "test": [Engine.Aurora],
            "test2": [Engine.Redshift],
            "test3": [Engine.Redshift],
        },
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 1),
        full_routing_policy=FullRoutingPolicy([], AlwaysOneRouter(Engine.Aurora)),
    )
    bp2 = Blueprint(
        "schema",
        table_schemas=[
            Table("test", [], [], None, []),
            Table("test2", [], [], None, []),
            Table("test3", [], [], None, []),
        ],
        table_locations={
            "test": [Engine.Aurora],
            "test2": [Engine.Redshift, Engine.Athena, Engine.Aurora],
            "test3": [Engine.Athena],
        },
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 1),
        full_routing_policy=FullRoutingPolicy([], AlwaysOneRouter(Engine.Aurora)),
    )

    bp_filter1 = SingleEngineExecution(workload1)
    bp_filter2 = SingleEngineExecution(workload2)

    assert not bp_filter1.is_valid(bp1)
    assert bp_filter1.is_valid(bp2)

    assert bp_filter2.is_valid(bp1)
    assert bp_filter2.is_valid(bp2)


def test_table_on_engine():
    bp_filter = TableOnEngine()

    bp1 = Blueprint(
        "schema",
        table_schemas=[
            Table("test", [], [], None, []),
            Table("test2", [], [], None, []),
            Table("test3", [], [], None, []),
        ],
        table_locations={
            "test": [Engine.Aurora],
            "test2": [Engine.Redshift],
            "test3": [Engine.Redshift],
        },
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 1),
        full_routing_policy=FullRoutingPolicy([], AlwaysOneRouter(Engine.Aurora)),
    )
    bp2 = Blueprint(
        "schema",
        table_schemas=[
            Table("test", [], [], None, []),
            Table("test2", [], [], None, []),
            Table("test3", [], [], None, []),
        ],
        table_locations={
            "test": [Engine.Aurora],
            "test2": [Engine.Redshift],
            "test3": [Engine.Redshift],
        },
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 0),
        full_routing_policy=FullRoutingPolicy([], AlwaysOneRouter(Engine.Aurora)),
    )
    bp3 = Blueprint(
        "schema",
        table_schemas=[
            Table("test", [], [], None, []),
            Table("test2", [], [], None, []),
            Table("test3", [], [], None, []),
        ],
        table_locations={
            "test": [Engine.Athena],
            "test2": [Engine.Athena],
            "test3": [Engine.Aurora],
        },
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 0),
        full_routing_policy=FullRoutingPolicy([], AlwaysOneRouter(Engine.Aurora)),
    )

    assert bp_filter.is_valid(bp1)
    assert not bp_filter.is_valid(bp2)
    assert bp_filter.is_valid(bp3)


def test_no_data_loss():
    ndl_filter = NoDataLoss()
    bp1 = Blueprint(
        "schema",
        table_schemas=[
            Table("test", [], [], None, []),
            Table("test2", [], [], None, []),
            Table("test3", [], [], None, []),
        ],
        table_locations={
            "test": [Engine.Aurora],
            "test2": [],
            "test3": [Engine.Redshift],
        },
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 1),
        full_routing_policy=FullRoutingPolicy([], AlwaysOneRouter(Engine.Aurora)),
    )
    bp2 = Blueprint(
        "schema",
        table_schemas=[
            Table("test", [], [], None, []),
            Table("test2", [], [], None, []),
            Table("test3", [], [], None, []),
        ],
        table_locations={
            "test": [Engine.Aurora],
            "test2": [Engine.Aurora],
            "test3": [Engine.Redshift],
        },
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 1),
        full_routing_policy=FullRoutingPolicy([], AlwaysOneRouter(Engine.Aurora)),
    )

    assert not ndl_filter.is_valid(bp1)
    assert ndl_filter.is_valid(bp2)
