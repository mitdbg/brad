from brad.planner.plan_parsing import parse_explain_verbose, extract_base_cardinalities


def get_rows():
    explain_verbose = """
    Finalize GroupAggregate  (cost=176426.88..176427.66 rows=3 width=40)
    Output: i_category, sum(i_price)
    Group Key: inventory.i_category
    ->  Gather Merge  (cost=176426.88..176427.58 rows=6 width=40)
          Output: i_category, (PARTIAL sum(i_price))
          Workers Planned: 2
          ->  Sort  (cost=175426.86..175426.87 rows=3 width=40)
                Output: i_category, (PARTIAL sum(i_price))
                Sort Key: inventory.i_category
                ->  Partial HashAggregate  (cost=175426.80..175426.83 rows=3 width=40)
                      Output: i_category, PARTIAL sum(i_price)
                      Group Key: inventory.i_category
                      ->  Parallel Seq Scan on public.inventory  (cost=0.00..154550.06 rows=4175348 width=16)
                            Output: i_id, i_name, i_category, i_stock, i_price, i_phys_id
                            Filter: (inventory.i_stock > 0)
    JIT:
    Functions: 9
    Options: Inlining false, Optimization false, Expressions true, Deforming true
    """
    explain_verbose_rows = explain_verbose.split("\n")[1:]
    return explain_verbose_rows


def get_redshift_rows():
    output = """
    XN HashAggregate  (cost=900020.46..900020.46 rows=19061 width=4)
    ->  XN Seq Scan on lineorder  (cost=0.00..750029.55 rows=59996364 width=4)
            Filter: (lo_quantity > 0)
    """
    rows = output.split("\n")[1:]
    return rows


def traverse_plan(plan, visitor):
    stack = [plan]
    while len(stack) > 0:
        op = stack.pop()
        visitor(op)
        if "children" in op:
            for child in op["children"]:
                stack.append(child)


def test_parse_explain():
    plan = parse_explain_verbose(get_rows())
    num_ops = 0

    def visitor(op):
        nonlocal num_ops
        num_ops += 1
        assert "est_card" in op["plan_parameters"]

    traverse_plan(plan, visitor)
    assert num_ops == 5


def test_redshift_parse():
    # A sanity check to make sure we can parse Redshift plans too (they look
    # very similar to PostgreSQL plans).
    plan = parse_explain_verbose(get_redshift_rows())
    num_ops = 0

    def visitor(op):
        nonlocal num_ops
        num_ops += 1
        assert "est_card" in op["plan_parameters"]

    traverse_plan(plan, visitor)
    assert num_ops == 2


def test_extract_base_cardinality():
    # PostgreSQL
    plan = parse_explain_verbose(get_rows())
    cards = extract_base_cardinalities(plan)
    assert len(cards) == 1
    assert cards[0].cardinality == 4175348
    assert cards[0].width == 16

    # Redshift
    plan = parse_explain_verbose(get_redshift_rows())
    cards = extract_base_cardinalities(plan)
    assert len(cards) == 1
    assert cards[0].cardinality == 59996364
    assert cards[0].width == 4
