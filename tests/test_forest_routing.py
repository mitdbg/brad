import numpy as np

from sklearn.ensemble import RandomForestClassifier

from brad.config.engine import Engine
from brad.routing.context import RoutingContext
from brad.routing.tree_based.forest_policy import ForestPolicy
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


def test_model_codepath_partial():
    model = get_dummy_router()
    router = ForestPolicy.from_loaded_model(RoutingPolicy.ForestTablePresence, model)
    ctx = RoutingContext()

    query = QueryRep("SELECT * FROM test1, test2")
    loc = router.engine_for_sync(query, ctx)
    assert (
        loc[0] == Engine.Aurora or loc[0] == Engine.Redshift or loc[0] == Engine.Athena
    )


def test_model_codepath_all():
    model = get_dummy_router()
    router = ForestPolicy.from_loaded_model(RoutingPolicy.ForestTablePresence, model)
    ctx = RoutingContext()

    query = QueryRep("SELECT * FROM test1")
    loc = router.engine_for_sync(query, ctx)
    assert (
        loc[0] == Engine.Aurora or loc[0] == Engine.Redshift or loc[0] == Engine.Athena
    )
