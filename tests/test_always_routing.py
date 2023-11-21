from brad.config.engine import Engine
from brad.routing.always_one import AlwaysOneRouter
from brad.routing.context import RoutingContext


def test_always_route_aurora():
    db = Engine.Aurora
    router = AlwaysOneRouter(db)
    ctx = RoutingContext()

    pred_db = router.engine_for_sync("SELECT 1", ctx)
    assert pred_db == [db]

    pred_db = router.engine_for_sync("SELECT * FROM my_table", ctx)
    assert pred_db == [db]


def test_always_route_athena():
    db = Engine.Athena
    router = AlwaysOneRouter(db)
    ctx = RoutingContext()

    pred_db = router.engine_for_sync("SELECT 1", ctx)
    assert pred_db == [db]

    pred_db = router.engine_for_sync("SELECT * FROM my_table", ctx)
    assert pred_db == [db]


def test_always_route_redshift():
    db = Engine.Redshift
    router = AlwaysOneRouter(db)
    ctx = RoutingContext()

    pred_db = router.engine_for_sync("SELECT 1", ctx)
    assert pred_db == [db]

    pred_db = router.engine_for_sync("SELECT * FROM my_table", ctx)
    assert pred_db == [db]
