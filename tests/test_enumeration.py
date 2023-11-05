from brad.blueprint import Blueprint
from brad.blueprint.provisioning import Provisioning
from brad.config.engine import Engine
from brad.planner.enumeration.provisioning import ProvisioningEnumerator
from brad.planner.enumeration.table_locations import TableLocationEnumerator
from brad.planner.enumeration.neighborhood import NeighborhoodBlueprintEnumerator
from brad.routing.abstract_policy import FullRoutingPolicy
from brad.routing.always_one import AlwaysOneRouter


def test_provisioning_enumerate_aurora():
    aurora = ProvisioningEnumerator(Engine.Aurora)
    base_aurora = Provisioning("db.r6g.large", 1)

    aurora_nearby = [
        p.clone()
        for p in aurora.enumerate_nearby(
            base_aurora, aurora.scaling_to_distance(base_aurora, 2, Engine.Aurora)
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
            base_redshift,
            redshift.scaling_to_distance(base_redshift, 2, Engine.Redshift),
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


def test_blueprint_enumerate():
    enumerator = NeighborhoodBlueprintEnumerator()
    base_bp = Blueprint(
        "test",
        [],
        {"table1": [Engine.Aurora], "table2": [Engine.Redshift]},
        aurora_provisioning=Provisioning("db.r6g.large", 1),
        redshift_provisioning=Provisioning("dc2.large", 1),
        full_routing_policy=FullRoutingPolicy([], AlwaysOneRouter(Engine.Aurora)),
    )

    # Simple sanity check only.
    last_bp = None
    for bp in enumerator.enumerate(base_bp, 2, 2.0):
        if last_bp is not None:
            assert last_bp != bp
        # Must call `.to_blueprint()` to get a copy of the blueprint because
        # enumeration is done in place.
        last_bp = bp.to_blueprint()
