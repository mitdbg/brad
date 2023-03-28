from brad.blueprint.data.user import UserProvidedDataBlueprint
from brad.planner.data import bootstrap_data_blueprint
from brad.blueprint.serde.data import (
    serialize_data_blueprint,
    deserialize_data_blueprint,
)


def test_data_blueprint_serde():
    table_config = """
      database_name: test
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
    blueprint_orig = bootstrap_data_blueprint(user)
    blueprint_after = deserialize_data_blueprint(
        serialize_data_blueprint(blueprint_orig)
    )

    # Sanity check assertions.
    assert blueprint_orig.schema_name == blueprint_after.schema_name
    assert len(blueprint_orig.table_schemas) == len(blueprint_after.table_schemas)
    assert len(blueprint_orig.table_locations) == len(blueprint_after.table_locations)
    assert len(blueprint_orig.table_dependencies) == len(
        blueprint_after.table_dependencies
    )
