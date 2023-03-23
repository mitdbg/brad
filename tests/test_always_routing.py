from brad.config.dbtype import DBType
from brad.cost_model.always_one import AlwaysOneCostModel


def test_always_route_aurora():
    db = DBType.Aurora
    model = AlwaysOneCostModel(db)

    pred = model.predict_run_time("SELECT 1")
    pred_db, _ = pred.min_time_ms()
    assert pred_db == db

    pred = model.predict_run_time("SELECT * FROM my_table")
    pred_db, _ = pred.min_time_ms()
    assert pred_db == db


def test_always_route_athena():
    db = DBType.Athena
    model = AlwaysOneCostModel(db)

    pred = model.predict_run_time("SELECT 1")
    pred_db, _ = pred.min_time_ms()
    assert pred_db == db

    pred = model.predict_run_time("SELECT * FROM my_table")
    pred_db, _ = pred.min_time_ms()
    assert pred_db == db


def test_always_route_redshift():
    db = DBType.Redshift
    model = AlwaysOneCostModel(db)

    pred = model.predict_run_time("SELECT 1")
    pred_db, _ = pred.min_time_ms()
    assert pred_db == db

    pred = model.predict_run_time("SELECT * FROM my_table")
    pred_db, _ = pred.min_time_ms()
    assert pred_db == db
