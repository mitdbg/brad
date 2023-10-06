import asyncio
import pytest

from brad.asset_manager import AssetManager
from brad.blueprint.manager import BlueprintManager
from brad.blueprint.user import UserProvidedBlueprint
from brad.config.file import ConfigFile
from brad.planner.data import bootstrap_blueprint
from brad.provisioning.physical import PhysicalProvisioning
from brad.daemon.monitor import Monitor


@pytest.mark.skip(reason="No way of running in CI. It needs to start actual clusters.")
def test_neighborhood_change():
    table_config = """
      schema_name: test
      tables:
        - table_name: table1
          columns:
            - name: col1
              data_type: BIGINT
              primary_key: true
        - table_name: table2
          columns:
            - name: col1
              data_type: BIGINT
              primary_key: true
          dependencies:
            - table1
        - table_name: table3
          columns:
            - name: col1
              data_type: BIGINT
              primary_key: true
      provisioning:
        aurora:
          num_nodes: 1
          instance_type: db.r6g.large
        redshift:
          num_nodes: 2
          instance_type: dc2.large
    """
    user = UserProvidedBlueprint.load_from_yaml_str(table_config)
    initial = bootstrap_blueprint(user)
    print()  # Flush stdout.
    print("Running test")
    config = ConfigFile(
        "config.yml"
    )  # TODO: Support configs in tests. This will not work.
    assets = AssetManager(config)
    blueprint_mgr = BlueprintManager(config, assets, "test")
    # N.B. This may not work.
    blueprint_mgr._current_blueprint = initial  # pylint: disable=protected-access
    monitor = Monitor(config, blueprint_mgr)
    asyncio.run(monitor.fetch_latest())  # Same effect as running for a long time.
    physical = PhysicalProvisioning(monitor=monitor, initial_blueprint=initial)
    # With default bounds, should be underutilized. (Only works if no concurrent benchmark running)
    print("TEST NEIGH")
    should_plan = physical.should_trigger_replan()
    assert should_plan
    # Set utilization within bound. Should not trigger replan.
    should_plan = physical.should_trigger_replan(
        overrides={
            "CPUUtilization_Average": 60,
            "os.cpuUtilization.total.avg": 60,
        }
    )
    assert not (should_plan)
    # Override specific metrics for testing.
    # Setting utilization to 100% should trigger a
    asyncio.run(monitor.fetch_latest())  # Same effect as run_forever after 5 minutes.
    should_plan = physical.should_trigger_replan(
        overrides={
            "os.cpuUtilization.total.avg": 100,
        }
    )
    assert should_plan
    # Pause
    # physical.pause_all()
