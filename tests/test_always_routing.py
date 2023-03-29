from brad.config.dbtype import DBType
from brad.routing.always_one import AlwaysOneRouter


def test_always_route_aurora():
    db = DBType.Aurora
    router = AlwaysOneRouter(db)

    pred_db = router.engine_for("SELECT 1")
    assert pred_db == db

    pred_db = router.engine_for("SELECT * FROM my_table")
    assert pred_db == db


def test_always_route_athena():
    db = DBType.Athena
    router = AlwaysOneRouter(db)

    pred_db = router.engine_for("SELECT 1")
    assert pred_db == db

    pred_db = router.engine_for("SELECT * FROM my_table")
    assert pred_db == db


def test_always_route_redshift():
    db = DBType.Redshift
    router = AlwaysOneRouter(db)

    pred_db = router.engine_for("SELECT 1")
    assert pred_db == db

    pred_db = router.engine_for("SELECT * FROM my_table")
    assert pred_db == db
