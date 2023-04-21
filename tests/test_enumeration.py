from brad.blueprint.provisioning import Provisioning
from brad.config.engine import Engine
from brad.planner.enumeration.provisioning import ProvisioningEnumerator
from brad.planner.enumeration.table_locations import TableLocationEnumerator


def test_provisioning_enumerate_aurora():
    aurora = ProvisioningEnumerator(Engine.Aurora)
    base_aurora = Provisioning("db.r6g.large", 1)

    aurora_nearby = [
        p.clone()
        for p in aurora.enumerate_nearby(
            base_aurora, aurora.scaling_to_distance(base_aurora, 2)
        )
    ]

    # Sanity check that we include the starting Aurora instance.
    assert any(map(lambda p: p == base_aurora, aurora_nearby))


def test_provisioning_enumerate_redshift():
    redshift = ProvisioningEnumerator(Engine.Redshift)
    base_redshift = Provisioning("dc2.large", 1)

    redshift_nearby = [
        p.clone()
        for p in redshift.enumerate_nearby(
            base_redshift, redshift.scaling_to_distance(base_redshift, 2)
        )
    ]

    # Sanity check that we include the starting Redshift instance.
    assert any(map(lambda p: p == base_redshift, redshift_nearby))


def test_table_placement_enumerate():
    tables = {"table": [Engine.Aurora]}

    # Simple sanity check assertions.
    count = 0
    for _ in TableLocationEnumerator.enumerate_nearby(tables, 3):
        count += 1
    assert count == 8

    count = 0
    for _ in TableLocationEnumerator.enumerate_nearby(tables, 2):
        count += 1
    assert count == 7

    count = 0
    for _ in TableLocationEnumerator.enumerate_nearby(tables, 1):
        count += 1
    assert count == 4
