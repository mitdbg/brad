import pytest

from brad.blueprint.user import UserProvidedBlueprint


def test_validate_ok():
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
    user.validate()


def test_dependency_on_undeclared_table():
    table_config = """
      schema_name: test
      tables:
        - table_name: table1
          columns:
            - name: col1
              data_type: BIGINT
              primary_key: true
          dependencies:
            - table2
    """
    user = UserProvidedBlueprint.load_from_yaml_str(table_config)
    with pytest.raises(RuntimeError) as ex:
        user.validate()
        assert "undeclared" in str(ex)


def test_circular_1():
    table_config = """
      schema_name: test
      tables:
        - table_name: table1
          columns:
            - name: col1
              data_type: BIGINT
              primary_key: true
          dependencies:
            - table1
    """
    user = UserProvidedBlueprint.load_from_yaml_str(table_config)
    with pytest.raises(RuntimeError) as ex:
        user.validate()
        assert "circular" in str(ex)


def test_circular_2():
    table_config = """
      schema_name: test
      tables:
        - table_name: table1
          columns:
            - name: col1
              data_type: BIGINT
              primary_key: true
          dependencies:
            - table2

        - table_name: table2
          columns:
            - name: col1
              data_type: BIGINT
              primary_key: true
          dependencies:
            - table1
    """
    user = UserProvidedBlueprint.load_from_yaml_str(table_config)
    with pytest.raises(RuntimeError) as ex:
        user.validate()
        assert "circular" in str(ex)


def test_circular_3():
    table_config = """
      schema_name: test
      tables:
        - table_name: table1
          columns:
            - name: col1
              data_type: BIGINT
              primary_key: true
          dependencies:
            - table3

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
          dependencies:
            - table2
    """
    user = UserProvidedBlueprint.load_from_yaml_str(table_config)
    with pytest.raises(RuntimeError) as ex:
        user.validate()
        assert "circular" in str(ex)
