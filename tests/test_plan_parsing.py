from brad.data_stats.plan_parsing import (
    parse_explain_verbose,
    extract_base_cardinalities,
)


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


def get_complex_rows():
    output = """
->  Finalize Aggregate  (cost=819846.70..819846.71 rows=1 width=8)
  Output: min((title_brad_source.episode_of_id + cast_info_brad_source.role_id))
  ->  Gather  (cost=819846.49..819846.70 rows=2 width=8)
        Output: (PARTIAL min((title_brad_source.episode_of_id + cast_info_brad_source.role_id)))
        Workers Planned: 2
        ->  Partial Aggregate  (cost=818846.49..818846.50 rows=1 width=8)
              Output: PARTIAL min((title_brad_source.episode_of_id + cast_info_brad_source.role_id))
              ->  Parallel Hash Join  (cost=752058.36..813810.92 rows=1007114 width=16)
                    Output: title_brad_source.episode_of_id, cast_info_brad_source.role_id
                    Inner Unique: true
                    Hash Cond: (movie_companies_brad_source.company_id = company_name_brad_source.id)
                    ->  Parallel Hash Join  (cost=745926.32..804967.76 rows=1032793 width=24)
                          Output: cast_info_brad_source.role_id, title_brad_source.episode_of_id, movie_companies_brad_source.company_id
                          Hash Cond: (movie_companies_brad_source.movie_id = cast_info_brad_source.movie_id)
                          ->  Parallel Seq Scan on public.movie_companies_brad_source  (cost=0.00..37380.37 rows=1087137 width=16)
                                Output: movie_companies_brad_source.movie_id, movie_companies_brad_source.company_id
                          ->  Parallel Hash  (cost=738394.32..738394.32 rows=389520 width=28)
                                Output: cast_info_brad_source.role_id, cast_info_brad_source.movie_id, title_brad_source.episode_of_id, title_brad_source.id
                                ->  Parallel Hash Left Join  (cost=654405.34..738394.32 rows=389520 width=28)
                                      Output: cast_info_brad_source.role_id, cast_info_brad_source.movie_id, title_brad_source.episode_of_id, title_brad_source.id
                                      Hash Cond: (title_brad_source.id = movie_info_idx_brad_source.movie_id)
                                      ->  Parallel Hash Join  (cost=627219.51..701289.12 rows=389520 width=28)
                                            Output: cast_info_brad_source.role_id, cast_info_brad_source.movie_id, title_brad_source.episode_of_id, title_brad_source.id
                                            Hash Cond: (title_brad_source.id = cast_info_brad_source.movie_id)
                                            ->  Parallel Seq Scan on public.title_brad_source  (cost=0.00..54243.17 rows=1053717 width=20)
                                                  Output: title_brad_source.episode_of_id, title_brad_source.id, title_brad_source.kind_id
                                            ->  Parallel Hash  (cost=620448.51..620448.51 rows=389520 width=16)
                                                  Output: cast_info_brad_source.role_id, cast_info_brad_source.movie_id
                                                  ->  Parallel Hash Join  (cost=41591.54..620448.51 rows=389520 width=16)
                                                        Output: cast_info_brad_source.role_id, cast_info_brad_source.movie_id
                                                        Hash Cond: (cast_info_brad_source.person_id = aka_name_brad_source.id)
                                                        ->  Parallel Seq Scan on public.cast_info_brad_source  (cost=0.00..520642.77 rows=15090977 width=32)
                                                              Output: cast_info_brad_source.role_id, cast_info_brad_source.person_id, cast_info_brad_source.movie_id, cast_info_brad_source.person_role_id
                                                        ->  Parallel Hash  (cost=41470.37..41470.37 rows=9694 width=4)
                                                              Output: aka_name_brad_source.id
                                                              ->  Nested Loop  (cost=531.32..41470.37 rows=9694 width=4)
                                                                    Output: aka_name_brad_source.id
                                                                    Inner Unique: true
                                                                    ->  Parallel Bitmap Heap Scan on public.aka_name_brad_source  (cost=530.89..15285.07 rows=9694 width=12)
                                                                          Output: aka_name_brad_source.person_id, aka_name_brad_source.id
                                                                          Recheck Cond: ((aka_name_brad_source.person_id >= 1276226) AND (aka_name_brad_source.person_id <= 1377150))
                                                                          ->  Bitmap Index Scan on aka_name_person_id_index  (cost=0.00..525.07 rows=23265 width=0)
                                                                                Index Cond: ((aka_name_brad_source.person_id >= 1276226) AND (aka_name_brad_source.person_id <= 1377150))
                                                                    ->  Index Only Scan using name_brad_source_pkey on public.name_brad_source  (cost=0.43..2.70 rows=1 width=4)
                                                                          Output: name_brad_source.id
                                                                          Index Cond: (name_brad_source.id = aka_name_brad_source.person_id)
                                      ->  Parallel Hash  (cost=17751.15..17751.15 rows=575015 width=8)
                                            Output: movie_info_idx_brad_source.movie_id
                                            ->  Parallel Seq Scan on public.movie_info_idx_brad_source  (cost=0.00..17751.15 rows=575015 width=8)
                                                  Output: movie_info_idx_brad_source.movie_id
                    ->  Parallel Hash  (cost=4938.52..4938.52 rows=95481 width=4)
                          Output: company_name_brad_source.id
                          ->  Parallel Seq Scan on public.company_name_brad_source  (cost=0.00..4938.52 rows=95481 width=4)
                                Output: company_name_brad_source.id
                                Filter: (((company_name_brad_source.name)::text !~~ '%En%tertainment%'::text) OR ((company_name_brad_source.country_code)::text = ANY ('{[ca],[gb],[fr],[in]}'::text[])))
    """
    rows = output.split("\n")[1:]
    return rows


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


def test_complex_extract_base_cardinality():
    plan = parse_explain_verbose(get_complex_rows())
    cards = extract_base_cardinalities(plan)

    expected_tables = {
        "company_name": 0,
        "movie_info_idx": 0,
        "name": 0,
        "aka_name": 0,
        "cast_info": 0,
        "title": 0,
        "movie_companies": 0,
    }
    suffix = "_brad_source"
    suffix_len = len(suffix)

    for c in cards:
        assert c.table_name.endswith(suffix)
        clean_name = c.table_name[:-suffix_len]
        assert clean_name in expected_tables
        expected_tables[clean_name] += 1
    for tbl, value in expected_tables.items():
        assert value == 1, tbl
