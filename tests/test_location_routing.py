from brad.config.engine import Engine, EngineBitmapValues
from brad.routing.router import Router
from brad.routing.round_robin import RoundRobin
from brad.query_rep import QueryRep


def test_only_one_location():
    query = QueryRep("SELECT * FROM test")
    bitmap = {"test": EngineBitmapValues[Engine.Aurora]}
    r = Router.create_from_definite_policy(RoundRobin(), bitmap)
    # pylint: disable-next=protected-access
    valid_locations, only_location = r._run_location_routing(query, bitmap)
    assert only_location is not None
    assert only_location == Engine.Aurora
    assert valid_locations == EngineBitmapValues[Engine.Aurora]


def test_multiple_locations():
    query = QueryRep("SELECT * FROM test1, test2")
    bitmap = {
        "test1": Engine.bitmap_all(),
        "test2": (
            EngineBitmapValues[Engine.Redshift] | EngineBitmapValues[Engine.Athena]
        ),
    }
    r = Router.create_from_definite_policy(RoundRobin(), bitmap)
    # pylint: disable-next=protected-access
    valid_locations, only_location = r._run_location_routing(query, bitmap)
    assert only_location is None
    assert (valid_locations & EngineBitmapValues[Engine.Aurora]) == 0
    assert (valid_locations & EngineBitmapValues[Engine.Redshift]) != 0
    assert (valid_locations & EngineBitmapValues[Engine.Athena]) != 0
