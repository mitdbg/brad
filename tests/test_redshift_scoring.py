import pytest
from datetime import timedelta

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.config.planner import PlannerConfig
from brad.planner.metrics import Metrics
from brad.planner.scoring.context import ScoringContext
from brad.planner.scoring.performance.unified_redshift import RedshiftProvisioningScore
from brad.planner.scoring.provisioning import Provisioning
from brad.planner.workload import Workload
from brad.planner.workload.query import Query
from brad.routing.router import FullRoutingPolicy
from brad.routing.round_robin import RoundRobin


def get_fixtures(redshift_cpu: float, redshift_prov: Provisioning) -> ScoringContext:
    metrics = Metrics(
        redshift_cpu_avg=redshift_cpu,
        aurora_writer_cpu_avg=0.0,
        aurora_reader_cpu_avg=0.0,
        aurora_writer_buffer_hit_pct_avg=100.0,
        aurora_reader_buffer_hit_pct_avg=100.0,
        aurora_writer_load_minute_avg=0.0,
        aurora_reader_load_minute_avg=0.0,
        txn_completions_per_s=10.0,
        txn_lat_s_p50=0.010,
        txn_lat_s_p90=0.020,
    )
    planner_config = PlannerConfig(
        {
            "redshift_initialize_load_fraction": 0.25,
        }
    )
    workload = Workload(timedelta(hours=1), [Query("SELECT 1")], [], {})
    blueprint = Blueprint(
        "test",
        [],
        {},
        Provisioning("db.r6g.xlarge", 1),
        redshift_prov,
        FullRoutingPolicy([], RoundRobin()),
    )
    ctx = ScoringContext("test", blueprint, workload, workload, metrics, planner_config)
    return ctx


def test_off_to_off() -> None:
    curr_prov = Provisioning("dc2.large", 0)
    next_prov = Provisioning("dc2.large", 0)
    ctx = get_fixtures(redshift_cpu=0.0, redshift_prov=curr_prov)
    cpu_denorm = RedshiftProvisioningScore.predict_cpu_denorm(
        curr_prov, next_prov, 1.0, ctx
    )
    assert cpu_denorm == pytest.approx(0.0)


def test_on_to_off() -> None:
    curr_prov = Provisioning("dc2.large", 2)
    next_prov = Provisioning("dc2.large", 0)
    ctx = get_fixtures(redshift_cpu=50.0, redshift_prov=curr_prov)
    cpu_denorm = RedshiftProvisioningScore.predict_cpu_denorm(
        curr_prov, next_prov, 1.0, ctx
    )
    assert cpu_denorm == pytest.approx(0.0)


def test_off_to_on() -> None:
    curr_prov = Provisioning("dc2.large", 0)
    next_prov = Provisioning("dc2.large", 2)
    ctx = get_fixtures(redshift_cpu=0.0, redshift_prov=curr_prov)
    cpu_denorm = RedshiftProvisioningScore.predict_cpu_denorm(
        curr_prov, next_prov, None, ctx
    )
    # Special case: we prime the load with a fraction.
    assert cpu_denorm == pytest.approx(
        2.0 * 2.0 * ctx.planner_config.redshift_initialize_load_fraction()
    )


def test_on_to_on() -> None:
    curr_prov = Provisioning("dc2.large", 2)
    next_prov = Provisioning("dc2.large", 4)
    ctx = get_fixtures(redshift_cpu=50.0, redshift_prov=curr_prov)

    # Scale up, no movement.
    cpu_denorm = RedshiftProvisioningScore.predict_cpu_denorm(
        curr_prov, next_prov, 1.0, ctx
    )
    assert cpu_denorm == pytest.approx(2.0)

    # Scale up, 2x movement.
    cpu_denorm = RedshiftProvisioningScore.predict_cpu_denorm(
        curr_prov, next_prov, 2.0, ctx
    )
    assert cpu_denorm == pytest.approx(4.0)

    # Scale up, 0.5x movement (we stay conservative).
    cpu_denorm = RedshiftProvisioningScore.predict_cpu_denorm(
        curr_prov, next_prov, 0.5, ctx
    )
    assert cpu_denorm == pytest.approx(2.0)

    # Scale down, no movement.
    smaller_prov = Provisioning("dc2.large", 1)
    cpu_denorm = RedshiftProvisioningScore.predict_cpu_denorm(
        curr_prov, smaller_prov, 1.0, ctx
    )
    assert cpu_denorm == pytest.approx(2.0)

    # Scale down, 2x movement.
    cpu_denorm = RedshiftProvisioningScore.predict_cpu_denorm(
        curr_prov, smaller_prov, 2.0, ctx
    )
    assert cpu_denorm == pytest.approx(4.0)

    # Scale down, 0.5x movement (stay conservative).
    cpu_denorm = RedshiftProvisioningScore.predict_cpu_denorm(
        curr_prov, smaller_prov, 0.5, ctx
    )
    assert cpu_denorm == pytest.approx(2.0)

    # Special case (no queries executed before, but now there are queries).
    ctx.current_query_locations[Engine.Redshift].append(0)
    cpu_denorm = RedshiftProvisioningScore.predict_cpu_denorm(
        curr_prov, next_prov, None, ctx
    )
    assert cpu_denorm == pytest.approx(1.0)