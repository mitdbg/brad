from brad.blueprint.data.location import Location
from brad.blueprint.data.user import UserProvidedDataBlueprint
from brad.blueprint.data.table import TableLocation
from brad.planner.data import bootstrap_data_blueprint


def test_boostrap_data_blueprint():
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
    blueprint = bootstrap_data_blueprint(user)

    tables = blueprint.table_names()
    assert len(tables) == 3
    assert "table1" in tables
    assert "table2" in tables
    assert "table3" in tables

    # See `bootstrap_data_blueprint()`'s docstring.
    table1_locs = blueprint.locations_of("table1")
    assert len(table1_locs) == 1
    assert Location.Aurora in table1_locs

    table2_locs = blueprint.locations_of("table2")
    assert len(table2_locs) == 2
    assert Location.Redshift in table2_locs
    assert Location.S3Iceberg in table2_locs

    table3_locs = blueprint.locations_of("table3")
    assert len(table3_locs) == 3
    assert Location.Aurora in table3_locs
    assert Location.Redshift in table3_locs
    assert Location.S3Iceberg in table3_locs

    # Table 1 is a base table and it is only present on Aurora.
    assert blueprint.dependencies_of(TableLocation("table1", Location.Aurora)) is None
    assert blueprint.dependencies_of(TableLocation("table1", Location.Redshift)) is None
    assert (
        blueprint.dependencies_of(TableLocation("table1", Location.S3Iceberg)) is None
    )

    # Table 3 is a base table but it is replicated.
    assert blueprint.dependencies_of(TableLocation("table3", Location.Aurora)) is None

    t3_redshift = blueprint.dependencies_of(TableLocation("table3", Location.Redshift))
    assert t3_redshift is not None
    assert len(t3_redshift.sources) == 1

    t3_s3 = blueprint.dependencies_of(TableLocation("table3", Location.S3Iceberg))
    assert t3_s3 is not None
    assert len(t3_s3.sources) == 1

    # Table 2 is dependent on table 1 and is present on Redshift and S3.
    assert blueprint.dependencies_of(TableLocation("table2", Location.Aurora)) is None

    t2_redshift = blueprint.dependencies_of(TableLocation("table2", Location.Redshift))
    assert t2_redshift is not None
    assert len(t2_redshift.sources) == 1

    t2_s3 = blueprint.dependencies_of(TableLocation("table2", Location.S3Iceberg))
    assert t2_s3 is not None
    assert len(t2_s3.sources) == 1
