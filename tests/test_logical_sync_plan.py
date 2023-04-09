from brad.blueprint.data.table import TableName
from brad.blueprint.data.location import Location
from brad.config.dbtype import DBType
from brad.data_sync.logical_plan import (
    ApplyDeltas,
    EmptyDeltas,
    ExtractDeltas,
    TransformDeltas,
    LogicalDataSyncPlan,
)


def test_propagation_simple():
    e = ExtractDeltas(TableName("test"))
    a = ApplyDeltas(e, TableName("test2"), Location.Aurora)
    plan = LogicalDataSyncPlan([e, a], [e])
    plan.reset_definitely_empty()
    e.set_definitely_empty(True)
    plan.propagate_definitely_empty()

    assert e.is_definitely_empty()
    assert a.is_definitely_empty()


def test_propagation():
    e1 = ExtractDeltas(TableName("test1"))
    e2 = ExtractDeltas(TableName("test2"))
    t1 = TransformDeltas([e1, e2], "", TableName("test3"), DBType.Redshift)
    t2 = TransformDeltas([t1], "", TableName("test4"), DBType.Redshift)

    a1r = ApplyDeltas(e1, TableName("test1"), Location.Redshift)
    a1a = ApplyDeltas(e1, TableName("test1"), Location.S3Iceberg)
    a2r = ApplyDeltas(e2, TableName("test2"), Location.Redshift)
    a2a = ApplyDeltas(e2, TableName("test2"), Location.S3Iceberg)
    a3r = ApplyDeltas(t1, TableName("test3"), Location.Redshift)
    a3a = ApplyDeltas(t1, TableName("test3"), Location.S3Iceberg)
    a4r = ApplyDeltas(t2, TableName("test4"), Location.Redshift)
    a4a = ApplyDeltas(t2, TableName("test4"), Location.S3Iceberg)

    all_ops = [e1, e2, t1, t2, a1r, a1a, a2r, a2a, a3r, a3a, a4r, a4a]
    plan = LogicalDataSyncPlan(all_ops, [e1, e2])
    plan.reset_definitely_empty()

    e1.set_definitely_empty(True)
    plan.propagate_definitely_empty()

    assert e1.is_definitely_empty()
    assert not e2.is_definitely_empty()

    assert not t1.is_definitely_empty()
    assert not t2.is_definitely_empty()

    assert a1r.is_definitely_empty()
    assert a1a.is_definitely_empty()
    assert not a2r.is_definitely_empty()
    assert not a2a.is_definitely_empty()
    assert not a3r.is_definitely_empty()
    assert not a3a.is_definitely_empty()
    assert not a4r.is_definitely_empty()
    assert not a4a.is_definitely_empty()

    # Now the whole plan should be "empty".
    plan.reset_definitely_empty()
    e1.set_definitely_empty(True)
    e2.set_definitely_empty(True)
    plan.propagate_definitely_empty()
    assert all(map(lambda op: op.is_definitely_empty, all_ops))


def test_pruning_simple():
    e = ExtractDeltas(TableName("test"))
    a = ApplyDeltas(e, TableName("test2"), Location.Aurora)
    plan = LogicalDataSyncPlan([e, a], [e])
    plan.reset_definitely_empty()
    e.set_definitely_empty(True)
    plan.propagate_definitely_empty()
    pruned = plan.prune_empty_ops()

    assert len(pruned.base_operators()) == 0
    assert len(pruned.operators()) == 0


def test_pruning():
    e1 = ExtractDeltas(TableName("test1"))
    e2 = ExtractDeltas(TableName("test2"))
    t1 = TransformDeltas([e1, e2], "", TableName("test3"), DBType.Redshift)
    t2 = TransformDeltas([t1], "", TableName("test4"), DBType.Redshift)

    a1r = ApplyDeltas(e1, TableName("test1"), Location.Redshift)
    a1a = ApplyDeltas(e1, TableName("test1"), Location.S3Iceberg)
    a2r = ApplyDeltas(e2, TableName("test2"), Location.Redshift)
    a2a = ApplyDeltas(e2, TableName("test2"), Location.S3Iceberg)
    a3r = ApplyDeltas(t1, TableName("test3"), Location.Redshift)
    a3a = ApplyDeltas(t1, TableName("test3"), Location.S3Iceberg)
    a4r = ApplyDeltas(t2, TableName("test4"), Location.Redshift)
    a4a = ApplyDeltas(t2, TableName("test4"), Location.S3Iceberg)

    all_ops = [e1, e2, t1, t2, a1r, a1a, a2r, a2a, a3r, a3a, a4r, a4a]
    plan = LogicalDataSyncPlan(all_ops, [e1, e2])
    plan.reset_definitely_empty()
    e1.set_definitely_empty(True)
    plan.propagate_definitely_empty()
    pruned = plan.prune_empty_ops()

    num_extract = 0
    num_empty = 0
    num_transform = 0
    num_apply = 0
    for op in pruned.operators():
        if isinstance(op, ExtractDeltas):
            num_extract += 1
        elif isinstance(op, EmptyDeltas):
            num_empty += 1
        elif isinstance(op, TransformDeltas):
            num_transform += 1
        elif isinstance(op, ApplyDeltas):
            num_apply += 1

    # `ApplyDeltas` associated with `e1` are removed. `e1` should be replaced
    # with an `EmptyDeltas` op.
    assert num_extract == 1
    assert num_empty == 1
    assert num_apply == 6
    assert num_transform == 2

    # Now the whole plan should be "empty".
    plan.reset_definitely_empty()
    e1.set_definitely_empty(True)
    e2.set_definitely_empty(True)
    plan.propagate_definitely_empty()
    pruned = plan.prune_empty_ops()
    assert len(pruned.base_operators()) == 0
    assert len(pruned.operators()) == 0
