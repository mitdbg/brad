from brad.blueprint import Blueprint
from brad.blueprint.provisioning import Provisioning
from brad.blueprint.table import Table
from brad.config.engine import Engine
from brad.planner.workload import Workload
from brad.planner.workload.query_template import QueryTemplate
from brad.planner.filters.aurora_transactions import AuroraTransactions
from brad.planner.filters.single_engine_execution import SingleEngineExecution
from brad.planner.filters.table_on_engine import TableOnEngine


def test_aurora_transactions():
    workload1 = Workload(
        [QueryTemplate(tables=["test", "test2"], is_transactional=True)]
    )
    workload2 = Workload(
        [
            QueryTemplate(tables=["test"], is_transactional=False),
            QueryTemplate(tables=["test2"], is_transactional=False),
        ]
    )

    bp1 = Blueprint(
        "schema",
        table_schemas=[
            Table("test", [], [], None),
            Table("test2", [], [], None),
        ],
        table_locations={
            "test": [Engine.Aurora],
            "test2": [Engine.Redshift, Engine.Athena],
        },
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 1),
        router_provider=None,
    )
    bp2 = Blueprint(
        "schema",
        table_schemas=[
            Table("test", [], [], None),
            Table("test2", [], [], None),
        ],
        table_locations={
            "test": [Engine.Aurora],
            "test2": [Engine.Redshift, Engine.Athena, Engine.Aurora],
        },
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 1),
        router_provider=None,
    )

    bp_filter1 = AuroraTransactions(workload1)
    assert not bp_filter1.is_valid(bp1)
    assert bp_filter1.is_valid(bp2)

    bp_filter2 = AuroraTransactions(workload2)
    assert bp_filter2.is_valid(bp1)
    assert bp_filter2.is_valid(bp2)


def test_single_engine_execution():
    workload1 = Workload(
        [
            QueryTemplate(tables=["test", "test2"], is_transactional=False),
            QueryTemplate(tables=["test2", "test3"], is_transactional=False),
        ]
    )
    workload2 = Workload(
        [
            QueryTemplate(tables=["test"], is_transactional=False),
            QueryTemplate(tables=["test2"], is_transactional=False),
            QueryTemplate(tables=["test3"], is_transactional=False),
        ]
    )

    bp1 = Blueprint(
        "schema",
        table_schemas=[
            Table("test", [], [], None),
            Table("test2", [], [], None),
            Table("test3", [], [], None),
        ],
        table_locations={
            "test": [Engine.Aurora],
            "test2": [Engine.Redshift],
            "test3": [Engine.Redshift],
        },
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 1),
        router_provider=None,
    )
    bp2 = Blueprint(
        "schema",
        table_schemas=[
            Table("test", [], [], None),
            Table("test2", [], [], None),
            Table("test3", [], [], None),
        ],
        table_locations={
            "test": [Engine.Aurora],
            "test2": [Engine.Redshift, Engine.Athena, Engine.Aurora],
            "test3": [Engine.Athena],
        },
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 1),
        router_provider=None,
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
            Table("test", [], [], None),
            Table("test2", [], [], None),
            Table("test3", [], [], None),
        ],
        table_locations={
            "test": [Engine.Aurora],
            "test2": [Engine.Redshift],
            "test3": [Engine.Redshift],
        },
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 1),
        router_provider=None,
    )
    bp2 = Blueprint(
        "schema",
        table_schemas=[
            Table("test", [], [], None),
            Table("test2", [], [], None),
            Table("test3", [], [], None),
        ],
        table_locations={
            "test": [Engine.Aurora],
            "test2": [Engine.Redshift],
            "test3": [Engine.Redshift],
        },
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 0),
        router_provider=None,
    )
    bp3 = Blueprint(
        "schema",
        table_schemas=[
            Table("test", [], [], None),
            Table("test2", [], [], None),
            Table("test3", [], [], None),
        ],
        table_locations={
            "test": [Engine.Athena],
            "test2": [Engine.Athena],
            "test3": [Engine.Aurora],
        },
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 0),
        router_provider=None,
    )

    assert bp_filter.is_valid(bp1)
    assert not bp_filter.is_valid(bp2)
    assert bp_filter.is_valid(bp3)
