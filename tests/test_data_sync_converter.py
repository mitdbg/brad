from brad.blueprint.data.user import UserProvidedDataBlueprint
from brad.data_sync.execution.plan_converter import PlanConverter
from brad.planner.data import bootstrap_data_blueprint
from brad.planner.data_sync import make_logical_data_sync_plan


def test_data_sync_converter_sanity_check():
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

    converter = PlanConverter(dsp, blueprint)
    _ = converter.get_plan()


def test_data_sync_converter_sanity_check2():
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

    converter = PlanConverter(dsp, blueprint)
    _ = converter.get_plan()
