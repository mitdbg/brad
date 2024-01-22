from brad.blueprint.provisioning import Provisioning
from brad.daemon.redshift_metrics import relevant_redshift_node_dimensions
from brad.daemon.cloudwatch import MAX_REDSHIFT_NODES


def test_single_node():
    single = Provisioning("dc2.large", 1)
    expected, discard = relevant_redshift_node_dimensions(single)
    assert expected == ["Shared"]
    assert len(discard) == MAX_REDSHIFT_NODES + 1  # (plus Shared)


def test_multiple_nodes():
    multiple2 = Provisioning("dc2.large", 2)
    expected2, discard2 = relevant_redshift_node_dimensions(multiple2)
    assert "Leader" in expected2
    assert "Compute0" in expected2
    assert "Compute1" in expected2
    assert len(expected2) == 3
    assert "Shared" in discard2
    assert len(discard2) == MAX_REDSHIFT_NODES - 2 + 1

    multiple8 = Provisioning("dc2.large", 8)
    expected8, discard8 = relevant_redshift_node_dimensions(multiple8)
    assert "Leader" in expected8
    assert "Compute0" in expected8
    assert "Compute1" in expected8
    assert "Compute7" in expected8
    assert len(expected8) == 8 + 1
    assert "Shared" in discard8
    assert len(discard8) == MAX_REDSHIFT_NODES - 8 + 1
