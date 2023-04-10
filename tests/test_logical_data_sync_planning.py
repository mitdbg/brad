from brad.blueprint.user import UserProvidedDataBlueprint
from brad.config.engine import Engine
from brad.data_sync.logical_plan import ExtractDeltas, TransformDeltas, ApplyDeltas
from brad.planner.data import bootstrap_data_blueprint
from brad.planner.data_sync import make_logical_data_sync_plan


def test_make_logical_data_sync_plan_simple():
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
    user = UserProvidedDataBlueprint.load_from_yaml_str(table_config)
    blueprint = bootstrap_data_blueprint(user)
    dsp = make_logical_data_sync_plan(blueprint)

    base_tables = []
    for op in dsp.base_operators():
        # Base ops should always be "extract deltas"
        assert isinstance(op, ExtractDeltas)
        base_tables.append(op.table_name())

    # Table 1 is a base table.
    assert "table1" in base_tables

    # Table 2 is not a base table.
    assert "table2" not in base_tables

    table3 = "table3"
    table3_locations = blueprint.get_table(table3).locations

    if len(table3_locations) > 1 and Engine.Aurora in table3_locations:
        # Need to extract from it for replication.
        assert table3 in base_tables
    else:
        assert table3 not in base_tables

    # Should be no transforms because table2 is just a replica of table1.
    for op in dsp.operators():
        assert not isinstance(op, TransformDeltas)

    # Should be applying deltas to table2.
    apply_deltas_to = set()
    for op in dsp.operators():
        if isinstance(op, ApplyDeltas):
            apply_deltas_to.add(op.table_name())
    assert "table2" in apply_deltas_to


def test_make_logical_data_sync_plan_transforms():
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
          transform: |
            -- Some transformation code...
    """
    user = UserProvidedDataBlueprint.load_from_yaml_str(table_config)
    blueprint = bootstrap_data_blueprint(user)
    dsp = make_logical_data_sync_plan(blueprint)

    base_tables = []
    for op in dsp.base_operators():
        # Base ops should always be "extract deltas"
        assert isinstance(op, ExtractDeltas)
        base_tables.append(op.table_name())

    # Table 1 is a base table.
    assert "table1" in base_tables

    # Table 2 is not a base table.
    assert "table2" not in base_tables

    # There should be a transform.
    transforms = []
    for op in dsp.operators():
        if isinstance(op, TransformDeltas):
            transforms.append(op)
    assert len(transforms) == 1

    # Should be applying deltas to table2.
    apply_deltas_to = set()
    for op in dsp.operators():
        if isinstance(op, ApplyDeltas):
            apply_deltas_to.add(op.table_name())
    assert "table2" in apply_deltas_to
