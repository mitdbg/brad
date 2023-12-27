import pytest
import numpy as np
from datetime import timedelta
from typing import List

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


def get_fixtures(
    redshift_cpu: List[float], redshift_prov: Provisioning
) -> ScoringContext:
    cpus = np.array(redshift_cpu)
    metrics = Metrics(
        redshift_cpu_avg=cpus.max() if cpus.shape[0] > 0 else 0.0,
        aurora_writer_cpu_avg=0.0,
        aurora_reader_cpu_avg=0.0,
        aurora_writer_buffer_hit_pct_avg=100.0,
        aurora_reader_buffer_hit_pct_avg=100.0,
        aurora_writer_load_minute_avg=0.0,
        aurora_reader_load_minute_avg=0.0,
        txn_completions_per_s=10.0,
        txn_lat_s_p50=0.010,
        txn_lat_s_p90=0.020,
        query_lat_s_p50=10.0,
        query_lat_s_p90=20.0,
        redshift_cpu_list=cpus,
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
    ctx = get_fixtures(redshift_cpu=[], redshift_prov=curr_prov)
    cpu_util = RedshiftProvisioningScore.predict_max_node_cpu_util(
        curr_prov, next_prov, 1.0, ctx
    )
    assert cpu_util == pytest.approx(0.0)


def test_on_to_off() -> None:
    curr_prov = Provisioning("dc2.large", 2)
    next_prov = Provisioning("dc2.large", 0)
    ctx = get_fixtures(redshift_cpu=[50.0], redshift_prov=curr_prov)
    cpu_util = RedshiftProvisioningScore.predict_max_node_cpu_util(
        curr_prov, next_prov, 1.0, ctx
    )
    assert cpu_util == pytest.approx(0.0)


def test_off_to_on() -> None:
    curr_prov = Provisioning("dc2.large", 0)
    next_prov = Provisioning("dc2.large", 2)
    ctx = get_fixtures(redshift_cpu=[], redshift_prov=curr_prov)
    cpu_util = RedshiftProvisioningScore.predict_max_node_cpu_util(
        curr_prov, next_prov, None, ctx
    )
    # Special case: we prime the load with a fraction.
    assert cpu_util == pytest.approx(
        ctx.planner_config.redshift_initialize_load_fraction()
    )


def test_on_to_on() -> None:
    curr_prov = Provisioning("dc2.large", 2)
    next_prov = Provisioning("dc2.large", 4)
    ctx = get_fixtures(redshift_cpu=[50.0, 50.0], redshift_prov=curr_prov)

    # Scale up, no movement.
    cpu_util = RedshiftProvisioningScore.predict_max_node_cpu_util(
        curr_prov, next_prov, 1.0, ctx
    )
    assert cpu_util == pytest.approx(0.25)

    # Scale up, 2x movement.
    cpu_util = RedshiftProvisioningScore.predict_max_node_cpu_util(
        curr_prov, next_prov, 2.0, ctx
    )
    assert cpu_util == pytest.approx(0.5)

    # Scale up, 0.5x movement (we stay conservative).
    cpu_util = RedshiftProvisioningScore.predict_max_node_cpu_util(
        curr_prov, next_prov, 0.5, ctx
    )
    assert cpu_util == pytest.approx(2.0 * (1 - 0.25) / (4 * 2.0))

    # Scale down, no movement.
    smaller_prov = Provisioning("dc2.large", 1)
    cpu_util = RedshiftProvisioningScore.predict_max_node_cpu_util(
        curr_prov, smaller_prov, 1.0, ctx
    )
    assert cpu_util == pytest.approx(1.0)

    # Scale down, 2x movement.
    cpu_util = RedshiftProvisioningScore.predict_max_node_cpu_util(
        curr_prov, smaller_prov, 2.0, ctx
    )
    assert cpu_util == pytest.approx(1.0)

    # Scale down, 0.5x movement (stay conservative).
    cpu_util = RedshiftProvisioningScore.predict_max_node_cpu_util(
        curr_prov, smaller_prov, 0.5, ctx
    )
    assert cpu_util == pytest.approx(2.0 * (1 - 0.25) / 2.0)

    # Special case (no queries executed before, but now there are queries).
    ctx.current_query_locations[Engine.Redshift].append(0)
    cpu_util = RedshiftProvisioningScore.predict_max_node_cpu_util(
        curr_prov, next_prov, None, ctx
    )
    assert cpu_util == pytest.approx(0.25)
