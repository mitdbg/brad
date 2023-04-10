from brad.blueprint.data.user import UserProvidedDataBlueprint
from brad.config.dbtype import DBType
from brad.planner.data import bootstrap_data_blueprint


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
    user = UserProvidedDataBlueprint.load_from_yaml_str(table_config)
    blueprint = bootstrap_data_blueprint(user)

    tables = blueprint.tables
    table_names_str = list(map(lambda t: t.name, tables))
    assert len(tables) == 3
    assert "table1" in table_names_str
    assert "table2" in table_names_str
    assert "table3" in table_names_str

    table1 = blueprint.get_table("table1")
    assert len(table1.locations) == 2
    assert DBType.Aurora in table1.locations
    # Our heuristic replicates tables that are dependencies of others on
    # Redshift.
    assert DBType.Redshift in table1.locations

    table2 = blueprint.get_table("table2")
    assert len(table2.locations) == 2
    assert DBType.Redshift in table2.locations
    assert DBType.Athena in table2.locations

    table3 = blueprint.get_table("table3")
    assert len(table3.locations) == 3
    assert DBType.Aurora in table3.locations
    assert DBType.Redshift in table3.locations
    assert DBType.Athena in table3.locations

    # Table 1 is a base table and it is only present on Aurora.
    assert len(table1.table_dependencies) == 0

    # Table 3 is also a base table but it is replicated across Redshift and S3.
    assert len(table3.table_dependencies) == 0
    assert "table3" in blueprint.base_table_names

    # Table 2 is dependent on table 1 and is present on Redshift and S3.
    assert "table1" in table2.table_dependencies
    assert "table2" not in blueprint.base_table_names
