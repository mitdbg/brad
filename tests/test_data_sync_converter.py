from brad.blueprint.user import UserProvidedBlueprint
from brad.data_sync.execution.plan_converter import PlanConverter
from brad.data_sync.physical_plan import PhysicalDataSyncPlan
from brad.data_sync.planner import make_logical_data_sync_plan
from brad.planner.data import bootstrap_blueprint


def validate_physical_plan_structure(plan: PhysicalDataSyncPlan):
    # Traverse the plan and check that all operators are represented in the list
    # of operators.
    visited = set()
    stack = [*plan.base_ops()]

    while len(stack) > 0:
        op = stack.pop()
        if op in visited:
            continue
        visited.add(op)
        for dependee in op.dependees():
            stack.append(dependee)

    assert len(plan.all_operators()) == len(visited)
    for op in plan.all_operators():
        assert op in visited


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
    user = UserProvidedBlueprint.load_from_yaml_str(table_config)
    blueprint = bootstrap_blueprint(user)
    dsp = make_logical_data_sync_plan(blueprint)

    converter = PlanConverter(dsp, blueprint)
    plan = converter.get_plan()
    validate_physical_plan_structure(plan)
    plan.print_plan_sequentially()


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
    user = UserProvidedBlueprint.load_from_yaml_str(table_config)
    blueprint = bootstrap_blueprint(user)
    dsp = make_logical_data_sync_plan(blueprint)

    converter = PlanConverter(dsp, blueprint)
    plan = converter.get_plan()
    validate_physical_plan_structure(plan)
    plan.print_plan_sequentially()
