import pytest
from datetime import timedelta

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.config.planner import PlannerConfig
from brad.planner.metrics import Metrics
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.performance.unified_aurora import AuroraProvisioningScore
from brad.planner.scoring.provisioning import Provisioning
from brad.planner.workload import Workload
from brad.planner.workload.query import Query
from brad.routing.router import FullRoutingPolicy
from brad.routing.round_robin import RoundRobin


def get_fixtures(
    writer_cpu: float,
    reader_cpu: float,
    writer_load: float,
    reader_load: float,
    aurora_prov: Provisioning,
) -> ScoringContext:
    metrics = Metrics(
        redshift_cpu_avg=0.0,
        aurora_writer_cpu_avg=writer_cpu,
        aurora_reader_cpu_avg=reader_cpu,
        aurora_writer_buffer_hit_pct_avg=100.0,
        aurora_reader_buffer_hit_pct_avg=100.0,
        aurora_writer_load_minute_avg=writer_load,
        aurora_reader_load_minute_avg=reader_load,
        txn_completions_per_s=10.0,
        txn_lat_s_p50=0.010,
        txn_lat_s_p90=0.020,
        query_lat_s_p50=10.0,
        query_lat_s_p90=20.0,
    )
    planner_config = PlannerConfig(
        {
            "aurora_initialize_load_fraction": 0.25,
        }
    )
    workload = Workload(timedelta(hours=1), [Query("SELECT 1")], [], {})
    blueprint = Blueprint(
        "test",
        [],
        {},
        aurora_prov,
        Provisioning("dc2.large", 0),
        FullRoutingPolicy([], RoundRobin()),
    )
    ctx = ScoringContext("test", blueprint, workload, workload, metrics, planner_config)
    return ctx


def test_single_node() -> None:
    curr_prov = Provisioning("db.r6g.xlarge", 1)
    next_prov = Provisioning("db.r6g.2xlarge", 1)
    ctx = get_fixtures(
        writer_cpu=100.0,
        reader_cpu=0.0,
        writer_load=4.0,
        reader_load=0.0,
        aurora_prov=curr_prov,
    )

    # Scale up with no change in workload.
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, next_prov, 1.0, ctx
    )
    assert txn_cpu_denorm == pytest.approx(4.0)
    assert ana_load == pytest.approx(4.0)

    # Scale up with 2x increase in analytical workload.
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, next_prov, 2.0, ctx
    )
    assert txn_cpu_denorm == pytest.approx(8.0)
    assert ana_load == pytest.approx(8.0)

    # Scale up with a 0.5x increase (i.e., a decrease) in the analytical
    # workload. We stay conservative here and do not modify the loads.
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, next_prov, 0.5, ctx
    )
    assert txn_cpu_denorm == pytest.approx(4.0)
    assert ana_load == pytest.approx(4.0)

    # No queries executed previously. But now we are executing queries on
    # Aurora.
    ctx.current_query_locations[Engine.Aurora].append(0)
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, next_prov, None, ctx
    )
    assert txn_cpu_denorm == pytest.approx(
        4.0 + ctx.planner_config.aurora_initialize_load_fraction() * 4.0
    )
    assert ana_load == pytest.approx(
        4.0 + ctx.planner_config.aurora_initialize_load_fraction() * 4.0
    )


def test_single_to_multi() -> None:
    curr_prov = Provisioning("db.r6g.xlarge", 1)
    next_prov = Provisioning("db.r6g.xlarge", 2)
    ctx = get_fixtures(
        writer_cpu=100.0,
        reader_cpu=0.0,
        writer_load=4.0,
        reader_load=0.0,
        aurora_prov=curr_prov,
    )

    # Scale up with no change in workload.
    # We are conservative here and assume the same load is replicated across
    # both nodes.
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, next_prov, 1.0, ctx
    )
    assert txn_cpu_denorm == pytest.approx(4.0)
    assert ana_load == pytest.approx(4.0)

    # Scale up with 2x more load.
    # Transaction load should be unchanged because we move analytics to a
    # replica.
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, next_prov, 2.0, ctx
    )
    assert txn_cpu_denorm == pytest.approx(4.0)
    assert ana_load == pytest.approx(8.0)

    # Scale up with 0.5x more load.
    # We leave the loads unchanged because we are conservative.
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, next_prov, 0.5, ctx
    )
    assert txn_cpu_denorm == pytest.approx(4.0)
    assert ana_load == pytest.approx(4.0)

    # 2 replicas.
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, Provisioning("db.r6g.xlarge", 3), 1.0, ctx
    )
    assert txn_cpu_denorm == pytest.approx(4.0)
    # Should assume analytical load is split across the replicas.
    assert ana_load == pytest.approx(2.0)

    # No queries executed previously. But now we are executing queries on
    # Aurora.
    ctx.current_query_locations[Engine.Aurora].append(0)
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, next_prov, None, ctx
    )
    assert txn_cpu_denorm == pytest.approx(4.0)
    assert ana_load == pytest.approx(
        ctx.planner_config.aurora_initialize_load_fraction() * 4.0
    )


def test_multi_to_single() -> None:
    curr_prov = Provisioning("db.r6g.xlarge", 2)
    next_prov = Provisioning("db.r6g.xlarge", 1)

    ctx = get_fixtures(
        writer_cpu=50.0,
        reader_cpu=25.0,
        writer_load=2.0,
        reader_load=1.0,
        aurora_prov=curr_prov,
    )

    # Scale down with no change in workload.
    # All the load should be concentrated on the single node.
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, next_prov, 1.0, ctx
    )
    assert txn_cpu_denorm == pytest.approx(3.0)
    assert ana_load == pytest.approx(3.0)

    # Scale down with 2x more load.
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, next_prov, 2.0, ctx
    )
    assert txn_cpu_denorm == pytest.approx(2.0 + 2.0 * 1.0)
    assert ana_load == pytest.approx(2.0 + 2.0 * 1.0)

    # Scale down with 0.5x more load.
    # We stay conservative here.
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, next_prov, 0.5, ctx
    )
    assert txn_cpu_denorm == pytest.approx(3.0)
    assert ana_load == pytest.approx(3.0)

    # Multiple replicas (2) down to one node.
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        Provisioning("db.r6g.xlarge", 3), next_prov, 1.0, ctx
    )
    assert txn_cpu_denorm == pytest.approx(4.0)
    assert ana_load == pytest.approx(4.0)

    # Special case (no queries executed previously and now we are executing
    # queries). Should be rare because there are read replicas.
    ctx.current_query_locations[Engine.Aurora].append(0)
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, next_prov, None, ctx
    )
    assert txn_cpu_denorm == pytest.approx(
        2.0 + ctx.planner_config.aurora_initialize_load_fraction() * 4.0
    )
    assert ana_load == pytest.approx(
        2.0 + ctx.planner_config.aurora_initialize_load_fraction() * 4.0
    )


def test_multi_to_multi() -> None:
    # Doubling the number of replicas (2 to 4).
    curr_prov = Provisioning("db.r6g.xlarge", 3)
    next_prov = Provisioning("db.r6g.xlarge", 5)

    ctx = get_fixtures(
        writer_cpu=50.0,
        reader_cpu=25.0,
        writer_load=2.0,
        reader_load=1.0,
        aurora_prov=curr_prov,
    )

    # Scale up with no change in workload.
    # Load should be spread out.
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, next_prov, 1.0, ctx
    )
    assert txn_cpu_denorm == pytest.approx(2.0)
    assert ana_load == pytest.approx(0.5)

    # Scale up with a 2x change in workload.
    # Load should be spread out.
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, next_prov, 2.0, ctx
    )
    assert txn_cpu_denorm == pytest.approx(2.0)
    assert ana_load == pytest.approx(1.0)

    # Scale down with a 0.5x change in workload.
    # Load should be spread out.
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, next_prov, 0.5, ctx
    )
    assert txn_cpu_denorm == pytest.approx(2.0)
    assert ana_load == pytest.approx(0.5)  # Stay conservative.

    # Decrease the number of replicas (2 to 1).
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, Provisioning("db.r6g.xlarge", 2), 1.0, ctx
    )
    assert txn_cpu_denorm == pytest.approx(2.0)
    assert ana_load == pytest.approx(2.0)

    # Special case (no queries executed previously and now we are executing
    # queries). Should be rare because there are read replicas.
    ctx.current_query_locations[Engine.Aurora].append(0)
    txn_cpu_denorm, ana_load = AuroraProvisioningScore.predict_loads(
        curr_prov, next_prov, None, ctx
    )
    assert txn_cpu_denorm == pytest.approx(2.0)
    assert ana_load == pytest.approx(
        ctx.planner_config.aurora_initialize_load_fraction() * 4.0 * 2.0 / 4.0
    )
