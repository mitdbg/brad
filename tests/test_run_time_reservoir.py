from brad.utils.run_time_reservoir import RunTimeReservoir


def test_run_time_reservoir():
    rtr = RunTimeReservoir[int](3)
    rtr.add_value(1)
    rtr.add_value(3)
    rtr.add_value(2)
    rtr.add_value(2)

    summary = rtr.get_summary(k=2)
    assert summary.sum == 7
    assert summary.num_values == 3
    assert 2 in summary.top_k
    assert 3 in summary.top_k
    assert len(summary.top_k) == 2

    rtr.clear()
    rtr.add_value(5)
    summary = rtr.get_summary(k=2)
    assert summary.sum == 5
    assert summary.num_values == 1
    assert 5 in summary.top_k
    assert len(summary.top_k) == 1
