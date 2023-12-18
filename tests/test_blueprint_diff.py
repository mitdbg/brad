from brad.blueprint import Blueprint
from brad.blueprint.diff.blueprint import BlueprintDiff
from brad.blueprint.provisioning import Provisioning
from brad.blueprint.table import Table
from brad.blueprint.user import UserProvidedBlueprint
from brad.config.engine import Engine
from brad.planner.data import bootstrap_blueprint
from brad.routing.abstract_policy import FullRoutingPolicy
from brad.routing.always_one import AlwaysOneRouter


def test_no_diff():
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
    """
    user = UserProvidedBlueprint.load_from_yaml_str(table_config)
    blueprint = bootstrap_blueprint(user)
    diff = BlueprintDiff.of(blueprint, blueprint)
    assert diff is None


def test_provisioning_change():
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
          num_nodes: 1
          instance_type: dc2.large
    """
    user = UserProvidedBlueprint.load_from_yaml_str(table_config)
    initial = bootstrap_blueprint(user)
    changed = Blueprint(
        initial.schema_name(),
        initial.tables(),
        initial.table_locations(),
        initial.aurora_provisioning(),
        Provisioning(instance_type="dc2.large", num_nodes=4),
        full_routing_policy=initial.get_routing_policy(),
    )
    diff = BlueprintDiff.of(initial, changed)
    assert diff is not None
    assert len(diff.table_diffs()) == 0
    assert diff.aurora_diff() is None
    assert diff.redshift_diff() is not None
    assert not diff.has_routing_diff()

    redshift_diff = diff.redshift_diff()
    assert redshift_diff.new_instance_type() is None
    assert redshift_diff.new_num_nodes() == 4


def test_location_change():
    aurora = Provisioning(instance_type="db.r6g.large", num_nodes=1)
    redshift = Provisioning(instance_type="dc2.large", num_nodes=4)
    initial = Blueprint(
        "test",
        [
            Table(
                "table1",
                columns=[],
                table_dependencies=[],
                transform_text=None,
                secondary_indexed_columns=[],
            )
        ],
        {"table1": [Engine.Aurora]},
        aurora,
        redshift,
        None,
    )
    changed1 = Blueprint(
        "test",
        [
            Table(
                "table1",
                columns=[],
                table_dependencies=[],
                transform_text=None,
                secondary_indexed_columns=[],
            )
        ],
        {"table1": [Engine.Aurora, Engine.Redshift]},
        aurora,
        redshift,
        None,
    )

    diff1 = BlueprintDiff.of(initial, changed1)
    assert diff1 is not None
    assert diff1.aurora_diff() is None
    assert diff1.redshift_diff() is None
    assert len(diff1.table_diffs()) == 1
    assert not diff1.has_routing_diff()

    tdiff1 = diff1.table_diffs()[0]
    assert tdiff1.added_locations() == [Engine.Redshift]
    assert len(tdiff1.removed_locations()) == 0

    changed2 = Blueprint(
        "test",
        [
            Table(
                "table1",
                columns=[],
                table_dependencies=[],
                transform_text=None,
                secondary_indexed_columns=[],
            )
        ],
        {"table1": [Engine.Athena]},
        aurora,
        redshift,
        None,
    )

    diff2 = BlueprintDiff.of(initial, changed2)
    assert diff2 is not None
    assert diff2.aurora_diff() is None
    assert diff2.redshift_diff() is None
    assert len(diff2.table_diffs()) == 1
    assert not diff2.has_routing_diff()

    tdiff2 = diff2.table_diffs()[0]
    assert tdiff2.added_locations() == [Engine.Athena]
    assert tdiff2.removed_locations() == [Engine.Aurora]


def test_routing_policy_change():
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
          num_nodes: 1
          instance_type: dc2.large
    """
    user = UserProvidedBlueprint.load_from_yaml_str(table_config)
    initial = bootstrap_blueprint(user)
    changed = Blueprint(
        initial.schema_name(),
        initial.tables(),
        initial.table_locations(),
        initial.aurora_provisioning(),
        initial.redshift_provisioning(),
        full_routing_policy=FullRoutingPolicy([], AlwaysOneRouter(Engine.Athena)),
    )
    diff = BlueprintDiff.of(initial, changed)
    assert diff is not None
    assert len(diff.table_diffs()) == 0
    assert diff.aurora_diff() is None
    assert diff.redshift_diff() is None
    assert diff.has_routing_diff()
