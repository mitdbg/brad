from brad.blueprint.user import UserProvidedBlueprint
from brad.config.engine import Engine
from brad.planner.data import bootstrap_blueprint


def test_boostrap_data_blueprint():
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

    tables = blueprint.tables()
    table_names_str = list(map(lambda t: t.name, tables))
    assert len(tables) == 3
    assert "table1" in table_names_str
    assert "table2" in table_names_str
    assert "table3" in table_names_str

    table1_locations = blueprint.get_table_locations("table1")
    assert len(table1_locations) == 2
    assert Engine.Aurora in table1_locations
    # Our heuristic replicates tables that are dependencies of others on
    # Redshift.
    assert Engine.Redshift in table1_locations

    table2_locations = blueprint.get_table_locations("table2")
    assert len(table2_locations) == 2
    assert Engine.Redshift in table2_locations
    assert Engine.Athena in table2_locations

    table3_locations = blueprint.get_table_locations("table3")
    assert len(table3_locations) == 3
    assert Engine.Aurora in table3_locations
    assert Engine.Redshift in table3_locations
    assert Engine.Athena in table3_locations

    # Table 1 is a base table and it is only present on Aurora.
    table1 = blueprint.get_table("table1")
    assert len(table1.table_dependencies) == 0

    # Table 3 is also a base table but it is replicated across Redshift and S3.
    table3 = blueprint.get_table("table3")
    assert len(table3.table_dependencies) == 0
    assert "table3" in blueprint.base_table_names()

    # Table 2 is dependent on table 1 and is present on Redshift and S3.
    table2 = blueprint.get_table("table2")
    assert "table1" in table2.table_dependencies
    assert "table2" not in blueprint.base_table_names()
