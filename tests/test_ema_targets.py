"""EMA smoothing, "was it already broken" detection, and target selection."""
import pytest

import balancer as B
from conftest import fill, make_node, make_pod


# ------------------------------------------------------------------ update_ema

def test_first_sample_is_stored_as_is():
    node = fill(make_node(name="n1", actual_cpu_pct=40, actual_mem_pct=60), 10)
    state = {}

    B.update_ema(state, {"n1": node}, {})

    sample = state["ema"]["nodes"]["n1"]
    # EMA stores usage against allocatable, i.e. exactly what Node exposes
    assert sample["cpu"] == pytest.approx(node.act_cpu_pct, abs=0.5)
    assert sample["mem"] == pytest.approx(node.act_mem_pct, abs=0.5)
    assert sample["n"] == 1


def test_second_sample_is_smoothed_not_replaced():
    B.CFG["act_alpha"] = 0.35
    state = {}
    calm = fill(make_node(name="n1", actual_cpu_pct=40), 10)
    B.update_ema(state, {"n1": calm}, {})

    spike = fill(make_node(name="n1", actual_cpu_pct=90), 10)
    B.update_ema(state, {"n1": spike}, {})

    sample = state["ema"]["nodes"]["n1"]
    # 40 + (90-40)*0.35 = 57.5, not 90: one spike must not trigger a move
    assert round(sample["cpu"], 1) == 57.5
    assert sample["n"] == 2


def test_workload_usage_is_summed_across_replicas():
    node = fill(make_node(name="n1"), 2)
    a = make_pod("app-a", node="n1", owner_uid="rs-app")
    b = make_pod("app-b", node="n1", owner_uid="rs-app")
    for p in (a, b):
        node.add(p)
    node.finalize()
    act = {("app1", "app-a"): (0.5, 100 * 1024 ** 2), ("app1", "app-b"): (0.5, 100 * 1024 ** 2)}
    state = {}

    B.update_ema(state, {"n1": node}, act)

    workload = state["ema"]["workloads"]
    key = [k for k in workload if k.endswith("app")][0]
    assert round(workload[key]["cpu"], 2) == 1.0        # 0.5 + 0.5


# --------------------------------------------------- workload_already_bad

def _crashing(name, uid="rs-x", restarts=0, waiting=None):
    p = make_pod(name, node="n1", owner_uid=uid)
    status = {"restartCount": restarts, "ready": False}
    if waiting:
        status["state"] = {"waiting": {"reason": waiting}}
    p["status"]["containerStatuses"] = [status]
    return p


def test_a_crashing_sibling_marks_the_workload_as_already_bad():
    target = _crashing("app-1", uid="rs-x")
    sibling = _crashing("app-2", uid="rs-x", waiting="CrashLoopBackOff")
    node = fill(make_node(name="n1"), 0)
    node.add(sibling)
    node.finalize()

    assert B.workload_already_bad({"n1": node}, target) is True


def test_many_restarts_also_count_as_already_bad():
    target = _crashing("app-1", uid="rs-x")
    sibling = _crashing("app-2", uid="rs-x", restarts=7)
    node = fill(make_node(name="n1"), 0)
    node.add(sibling)
    node.finalize()

    assert B.workload_already_bad({"n1": node}, target) is True


def test_a_healthy_workload_is_not_flagged():
    target = _crashing("app-1", uid="rs-x")
    sibling = make_pod("app-2", node="n1", owner_uid="rs-x")
    node = fill(make_node(name="n1"), 0)
    node.add(sibling)
    node.finalize()

    assert B.workload_already_bad({"n1": node}, target) is False


def test_other_workloads_do_not_count():
    target = _crashing("app-1", uid="rs-x")
    other = _crashing("other-1", uid="rs-other", waiting="CrashLoopBackOff")
    node = fill(make_node(name="n1"), 0)
    node.add(other)
    node.finalize()

    assert B.workload_already_bad({"n1": node}, target) is False


def test_a_statefulset_is_judged_on_its_own_history():
    pod = _crashing("db-0", uid="st-1", restarts=5)
    assert B.workload_already_bad({}, pod, is_sts=True) is True

    fresh = _crashing("db-0", uid="st-1")
    assert B.workload_already_bad({}, fresh, is_sts=True) is False


# ---------------------------------------------------------------- pick_targets

def test_targets_exclude_the_source_and_this_round_sources():
    src = fill(make_node(name="src", group="pool-a"), 40)
    busy = fill(make_node(name="busy", group="pool-a"), 44)      # would be a source too
    idle = fill(make_node(name="idle", cpu=16, mem_gb=64, pods=211, group="pool-a"), 10)
    scope = {"src": src, "busy": busy, "idle": idle}
    B.ROUND_SOURCES.clear()
    B.ROUND_SOURCES.update({"src", "busy"})

    names = [n.name for n in B.pick_targets(src, scope)]

    assert names == ["idle"]


def test_targets_must_have_headroom():
    src = fill(make_node(name="src", group="pool-a"), 40)
    full = fill(make_node(name="full", group="pool-a"), 44)
    cordoned = fill(make_node(name="cordoned", group="pool-a", unschedulable=True), 5)
    scope = {"src": src, "full": full, "cordoned": cordoned}

    names = [n.name for n in B.pick_targets(src, scope)]

    assert names == []          # both are unusable as targets


def test_same_pool_targets_come_first():
    src = fill(make_node(name="src", group="pool-a"), 40)
    same = fill(make_node(name="same", group="pool-a"), 20)
    other = fill(make_node(name="other", cpu=16, mem_gb=64, pods=211, group="pool-b"), 0)
    scope = {"src": src, "same": same, "other": other}

    names = [n.name for n in B.pick_targets(src, scope)]

    assert names[0] == "same"


def test_cross_pool_targets_can_be_disabled():
    B.CFG["allow_cross_pool"] = False
    src = fill(make_node(name="src", group="pool-a"), 40)
    same = fill(make_node(name="same", group="pool-a"), 20)
    other = fill(make_node(name="other", cpu=16, mem_gb=64, pods=211, group="pool-b"), 0)
    scope = {"src": src, "same": same, "other": other}

    names = [n.name for n in B.pick_targets(src, scope)]

    assert "other" not in names
