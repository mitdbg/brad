from brad.config.engine import Engine
from brad.routing.always_one import AlwaysOneRouter


def test_always_route_aurora():
    db = Engine.Aurora
    router = AlwaysOneRouter(db)

    pred_db = router.engine_for("SELECT 1")
    assert pred_db == db

    pred_db = router.engine_for("SELECT * FROM my_table")
    assert pred_db == db


def test_always_route_athena():
    db = Engine.Athena
    router = AlwaysOneRouter(db)

    pred_db = router.engine_for("SELECT 1")
    assert pred_db == db

    pred_db = router.engine_for("SELECT * FROM my_table")
    assert pred_db == db


def test_always_route_redshift():
    db = Engine.Redshift
    router = AlwaysOneRouter(db)

    pred_db = router.engine_for("SELECT 1")
    assert pred_db == db

    pred_db = router.engine_for("SELECT * FROM my_table")
    assert pred_db == db
