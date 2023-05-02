from brad.planner.plan_parsing import parse_explain_verbose


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

    def visitor(_op):
        nonlocal num_ops
        num_ops += 1

    traverse_plan(plan, visitor)
    assert num_ops == 5
