import numpy as np

from sklearn.ensemble import RandomForestClassifier

from brad.config.engine import Engine, EngineBitmapValues
from brad.routing.tree_based.forest_router import ForestRouter
from brad.routing.tree_based.model_wrap import ModelWrap
from brad.routing.policy import RoutingPolicy
from brad.query_rep import QueryRep


def get_dummy_router():
    # Used just to verify the router's code paths.
    X = np.array([[0, 1], [1, 1], [1, 0]])
    y = np.array([0, 1, 2])
    clf = RandomForestClassifier(n_estimators=2, criterion="entropy")
    model = clf.fit(X, y)
    return ModelWrap(RoutingPolicy.ForestTablePresence, ["test1", "test2"], model)


def test_location_constraints():
    model = get_dummy_router()
    bitmap = {
        "test1": EngineBitmapValues[Engine.Aurora],
        "test2": EngineBitmapValues[Engine.Aurora]
        | EngineBitmapValues[Engine.Redshift],
    }
    router = ForestRouter.for_planner(
        RoutingPolicy.ForestTablePresence, "test_schema", model, bitmap
    )

    query1 = QueryRep("SELECT * FROM test1")
    loc = router.engine_for(query1)
    assert loc == Engine.Aurora

    query2 = QueryRep("SELECT * FROM test1, test2")
    loc = router.engine_for(query2)
    assert loc == Engine.Aurora


def test_model_codepath_partial():
    model = get_dummy_router()
    bitmap = {
        "test1": EngineBitmapValues[Engine.Aurora]
        | EngineBitmapValues[Engine.Redshift],
        "test2": EngineBitmapValues[Engine.Aurora]
        | EngineBitmapValues[Engine.Redshift],
    }
    router = ForestRouter.for_planner(
        RoutingPolicy.ForestTablePresence, "test_schema", model, bitmap
    )

    query = QueryRep("SELECT * FROM test1, test2")
    loc = router.engine_for(query)
    assert loc == Engine.Aurora or loc == Engine.Redshift


def test_model_codepath_all():
    model = get_dummy_router()
    bitmap = {
        "test1": Engine.bitmap_all(),
        "test2": EngineBitmapValues[Engine.Aurora]
        | EngineBitmapValues[Engine.Redshift],
    }
    router = ForestRouter.for_planner(
        RoutingPolicy.ForestTablePresence, "test_schema", model, bitmap
    )

    query = QueryRep("SELECT * FROM test1")
    loc = router.engine_for(query)
    assert loc == Engine.Aurora or loc == Engine.Redshift or loc == Engine.Athena
