from brad.query_rep import QueryRep


def test_is_data_modification():
    rep = QueryRep("SELECT 1")
    assert not rep.is_data_modification_query()

    rep = QueryRep("INSERT INTO test (a, b) VALUES (1, 2)")
    assert rep.is_data_modification_query()

    rep = QueryRep("INSERT INTO test (a, b) SELECT * FROM abc")
    assert rep.is_data_modification_query()

    rep = QueryRep("BEGIN")
    assert rep.is_data_modification_query()

    rep = QueryRep("COMMIT")
    assert rep.is_data_modification_query()

    rep = QueryRep("ROLLBACK")
    assert rep.is_data_modification_query()

    rep = QueryRep("UPDATE abc SET def = 123, ghi = 123")
    assert rep.is_data_modification_query()

    rep = QueryRep("DELETE FROM abc WHERE def = 1")
    assert rep.is_data_modification_query()

    rep = QueryRep("WITH test AS (SELECT * FROM abc) SELECT * FROM test")
    assert not rep.is_data_modification_query()


def test_extract_tables():
    rep = QueryRep("SELECT 1")
    assert len(rep.tables()) == 0

    rep = QueryRep("SELECT * FROM abc, def")
    tables = rep.tables()
    assert len(tables) == 2
    assert "abc" in tables
    assert "def" in tables

    rep = QueryRep("SELECT * FROM abc")
    tables = rep.tables()
    assert len(tables) == 1
    assert "abc" in tables

    rep = QueryRep("SELECT * FROM abc AS a JOIN def AS d ON a.id = d.id")
    tables = rep.tables()
    assert len(tables) == 2
    assert "abc" in tables
    assert "def" in tables

    rep = QueryRep("WITH test AS (SELECT * FROM abc) SELECT * FROM test")
    tables = rep.tables()
    # The CTE is treated as a table by our parser. This is not currently
    # problematic for routing, but if necessary later on, we can look into
    # possible fixes.
    assert len(tables) == 2
    assert "abc" in tables
    assert "test" in tables
