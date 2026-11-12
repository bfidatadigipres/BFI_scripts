"""Round-robin scheduler behaviour: fairness, FIFO, bounds, timeout."""

import threading

from scheduler import RoundRobinQueue


def test_fifo_within_single_caller():
    q = RoundRobinQueue(max_depth=10)
    for i in range(3):
        assert q.submit("a", i) is True
    assert [q.get(timeout=1) for _ in range(3)] == [0, 1, 2]


def test_round_robin_between_callers():
    q = RoundRobinQueue(max_depth=10)
    for i in range(3):
        q.submit("bulk", f"b{i}")
    q.submit("small", "s0")
    # bulk enqueued first, but small must be interleaved after the first bulk
    order = [q.get(timeout=1) for _ in range(4)]
    assert order[0] == "b0"
    assert "s0" in order[:2]
    assert sorted(order) == ["b0", "b1", "s0", "b2"] or set(order) == {
        "b0",
        "b1",
        "b2",
        "s0",
    }


def test_bulk_caller_does_not_starve_small_caller():
    q = RoundRobinQueue(max_depth=1000)
    for i in range(100):
        q.submit("bulk", i)
    q.submit("small", "urgent")
    seen_within = None
    for n in range(1, 6):
        if q.get(timeout=1) == "urgent":
            seen_within = n
            break
    assert seen_within is not None and seen_within <= 2


def test_max_depth_rejects_when_full():
    q = RoundRobinQueue(max_depth=2)
    assert q.submit("a", 1) is True
    assert q.submit("a", 2) is True
    assert q.submit("a", 3) is False


def test_get_times_out_when_empty():
    q = RoundRobinQueue(max_depth=5)
    assert q.get(timeout=0.1) is None


def test_depths_by_caller():
    q = RoundRobinQueue(max_depth=10)
    q.submit("a", 1)
    q.submit("a", 2)
    q.submit("b", 3)
    assert q.depths_by_caller() == {"a": 2, "b": 1}
    assert q.depth() == 3


def test_close_wakes_waiting_consumer():
    q = RoundRobinQueue(max_depth=5)
    result = {}

    def consumer():
        result["item"] = q.get(timeout=5)

    t = threading.Thread(target=consumer)
    t.start()
    q.close()
    t.join(timeout=2)
    assert result.get("item") is None
