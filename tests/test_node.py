"""Node-level decisions: which dimension drives a node, and may it receive a pod."""
import pytest

import balancer as B
from conftest import fill, make_node, make_pod, set_pool_usage


# --------------------------------------------------------------- driver()

def test_driver_none_when_everything_is_calm():
    n = fill(make_node(), 10)              # 10/46 slots, no requests, no usage
    assert n.driver() is None


def test_driver_pod_slots_win_over_actual_usage():
    n = fill(make_node(actual_mem_pct=95, actual_cpu_pct=95), 44)   # 95.7% slots
    dr = n.driver()
    assert dr is not None and dr[0] == "pod"


def test_driver_act_mem_when_memory_is_hot_but_slots_are_not():
    # physical basis: 96% of allocatable is ~86% of physical capacity, above MEM_ACT_HIGH=85
    n = fill(make_node(actual_mem_pct=96), 10)
    dr = n.driver()
    assert dr is not None and dr[0] == "act_mem"


def test_driver_act_cpu_when_cpu_is_hot():
    n = fill(make_node(actual_cpu_pct=70), 10)
    assert n.driver()[0] == "act_cpu"


def test_driver_requests_only_when_balance_dim_includes_requests():
    n = make_node()
    for i in range(10):
        n.add(make_pod(f"app-{i}", cpu=0.4, mem_mb=2000))   # 100% of 4 cpu / ~55% mem
    n.finalize()
    assert n.driver() is None or not n.driver()[0].startswith("req_")
    B.CFG["balance_dim"] = "both"
    dr = n.driver()
    assert dr is not None and dr[0].startswith("req_")


def test_driver_pool_pressure_marks_nodes_below_the_pod_waterline():
    """A pool above the line makes its fuller nodes sources even without a 95% node."""
    n = fill(make_node(), 40)                       # 87% slots, below POD_HIGH=95
    set_pool_usage(**{"pool-a/4C": 91.0, "pool-b/16C": 70.0})
    assert n.driver() == ("pool_slot", 40 / 46 * 100)


def test_driver_pool_pressure_only_pushes_the_fullest_pool():
    n = fill(make_node(), 40)
    set_pool_usage(**{"pool-a/4C": 84.0, "pool-b/16C": 92.0})
    assert n.driver() is None


def test_driver_pool_pressure_respects_its_own_floor():
    n = fill(make_node(), 30)                       # 65%: below POOL_PRESSURE_SRC_MIN=85
    set_pool_usage(**{"pool-a/4C": 91.0})
    assert n.driver() is None


def test_driver_pool_pressure_can_be_disabled():
    n = fill(make_node(), 40)
    B.CFG["pool_slot_pressure"] = 0
    set_pool_usage(**{"pool-a/4C": 99.0})
    assert n.driver() is None


# -------------------------------------------------------------- headroom

def test_headroom_ok_for_a_calm_node():
    assert fill(make_node(), 10).headroom_ok() is True


def test_headroom_rejects_unschedulable_nodes():
    """Someone else's cordon must never be used as a target."""
    assert make_node(unschedulable=True).headroom_ok() is False


def test_headroom_rejects_pressure_conditions():
    assert make_node(pressure=True).headroom_ok() is False


def test_headroom_rejects_not_ready():
    assert make_node(ready=False).headroom_ok() is False


def test_headroom_rejects_full_slot_nodes():
    assert fill(make_node(), 44).headroom_ok() is False      # 95.7% > TARGET_POD_MAX 90


# ------------------------------------------------------------ memory basis

def test_mem_guard_uses_physical_capacity_by_default():
    """80% of physical is 89% of allocatable — the alloc view would block this node."""
    n = fill(make_node(mem_gb=16, actual_mem_pct=78), 10)
    assert n.mem_guard_pct == pytest.approx(78.0, abs=0.01)
    assert n.act_mem_pct > 86            # the misleading alloc figure
    assert n.headroom_ok() is True


def test_mem_guard_switch_back_to_allocatable():
    B.CFG["target_memuse_basis"] = "alloc"
    n = fill(make_node(mem_gb=16, actual_mem_pct=95), 10)
    assert n.mem_guard_pct == pytest.approx(n.act_mem_pct, abs=0.01)
    assert n.headroom_ok() is False       # 95% of allocatable, above the 90 ceiling
