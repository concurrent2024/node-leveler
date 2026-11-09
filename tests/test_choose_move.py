"""Victim selection, quota gates and the pool-pressure special case."""
import balancer as B
from conftest import fill, make_node, make_pod, set_pool_usage


def _candidate(ns="app1", name="app1", owner_uid="rs-1", **kw):
    p = make_pod(name, ns=ns, owner_uid=owner_uid, **kw)
    p["_act"] = (0.01, 32 * 1024 ** 2)
    p["_act_known"] = True
    return p


def _world(src_pods, target_pods, *, single=True):
    src = fill(make_node(name="small", cpu=4, mem_gb=16, pods=46), src_pods - 1)
    pod = _candidate()
    src.add(pod)
    src.finalize()
    tgt = fill(make_node(name="large", cpu=16, mem_gb=64, pods=211, group="pool-b"), target_pods)
    rs = {pod["metadata"]["ownerReferences"][0]["uid"]: 1 if single else 2}
    return src, pod, tgt, rs, {"small": src, "large": tgt}


def _quota(**kw):
    q = {"cycle": 4, "single": 1, "sts": 0, "per_node": {}, "per_ns": {}, "daily": 100}
    q.update(kw)
    return q


def test_pool_pressure_moves_a_single_replica_across_node_shapes():
    """
    Regression: this exact geometry produced an entire round with zero moves.
    The target pool is emptier, so the node-percentage comparison must be skipped.
    """
    src, pod, tgt, rs, world = _world(40, 183)
    set_pool_usage(**{"pool-a/4C": 88.5, "pool-b/16C": 83.0})
    c, t = B.choose_move(src, "pool_slot", [tgt], rs, world, {"cooldowns": {}, "moves": []},
                         _quota(), set(), {})
    assert c is not None and c["pod"]["metadata"]["uid"] == pod["metadata"]["uid"]
    assert t.name == "large"


def test_slot_driver_keeps_the_strict_improvement_rule():
    """Same nodes, but a plain slot driver does not get the pool-pressure exemption."""
    src, _, tgt, rs, world = _world(40, 183)
    set_pool_usage(**{"pool-a/4C": 88.5, "pool-b/16C": 83.0})
    c, t = B.choose_move(src, "pod", [tgt], rs, world, {"cooldowns": {}, "moves": []},
                         _quota(), set(), {})
    assert c is None and t is None


def test_single_replica_quota_blocks_the_move():
    src, _, tgt, rs, world = _world(40, 183)
    set_pool_usage(**{"pool-a/4C": 88.5, "pool-b/16C": 83.0})
    c, _ = B.choose_move(src, "pool_slot", [tgt], rs, world, {"cooldowns": {}, "moves": []},
                         _quota(single=0), set(), {})
    assert c is None


def test_multi_replica_pods_are_not_limited_by_the_single_quota():
    src, _, tgt, rs, world = _world(40, 183, single=False)
    set_pool_usage(**{"pool-a/4C": 88.5, "pool-b/16C": 83.0})
    c, _ = B.choose_move(src, "pool_slot", [tgt], rs, world, {"cooldowns": {}, "moves": []},
                         _quota(single=0), set(), {})
    assert c is not None


def test_statefulsets_need_their_own_quota():
    src = fill(make_node(name="small", cpu=4, mem_gb=16, pods=46), 39)
    pod = make_pod("db-0", ns="app1", owner="StatefulSet", owner_name="db", owner_uid="st-1")
    pod["_act"] = (0.01, 64 * 1024 ** 2)
    pod["_act_known"] = True
    src.add(pod)
    src.finalize()
    tgt = fill(make_node(name="large", cpu=16, mem_gb=64, pods=211, group="pool-b"), 100)
    rs = {"st-1": 1}
    # ALLOW_STS_NS must be open, otherwise the pod is filtered out earlier
    B.CFG["allow_sts_ns"] = "^(app|batch)\\d+$"
    set_pool_usage(**{"pool-a/4C": 88.5, "pool-b/16C": 83.0})
    state = {"cooldowns": {}, "moves": []}
    assert B.choose_move(src, "pool_slot", [tgt], rs, {"small": src, "large": tgt},
                         state, _quota(sts=0, single=5), set(), {})[0] is None
    assert B.choose_move(src, "pool_slot", [tgt], rs, {"small": src, "large": tgt},
                         state, _quota(sts=1, single=5), set(), {})[0] is not None


def test_a_cooled_down_workload_is_skipped():
    src, _, tgt, rs, world = _world(40, 183)
    set_pool_usage(**{"pool-a/4C": 88.5, "pool-b/16C": 83.0})
    from balancer import workload_key
    key = workload_key(src.pods[-1])
    state = {"cooldowns": {key: B.now_ts() + 3600}, "moves": []}
    c, _ = B.choose_move(src, "pool_slot", [tgt], rs, world, state, _quota(), set(), {})
    assert c is None


def test_already_tried_pods_are_not_retried():
    src, pod, tgt, rs, world = _world(40, 183)
    set_pool_usage(**{"pool-a/4C": 88.5, "pool-b/16C": 83.0})
    c, _ = B.choose_move(src, "pool_slot", [tgt], rs, world, {"cooldowns": {}, "moves": []},
                         _quota(), {pod["metadata"]["uid"]}, {})
    assert c is None


def _big_world(src_pods=188, target_pods=150):
    """Two equal-shape nodes: one eviction is worth 0.47 percentage points."""
    src = fill(make_node(name="big-src", cpu=16, mem_gb=64, pods=211), src_pods - 1)
    pod = _candidate()
    src.add(pod)
    src.finalize()
    tgt = fill(make_node(name="big-tgt", cpu=16, mem_gb=64, pods=211), target_pods)
    rs = {pod["metadata"]["ownerReferences"][0]["uid"]: 1}
    return src, pod, tgt, rs, {"big-src": src, "big-tgt": tgt}


def test_slot_dimension_takes_no_benefit_floor():
    """Regression: a percentage-point floor is unreachable on large machines.

    One eviction on a 211-slot node relieves 0.47 points, so a 0.5 floor (or any value
    above it) used to reject every candidate and leave the whole pool frozen.
    """
    src, pod, tgt, rs, world = _big_world()
    set_pool_usage(**{"pool-a/16C": 89.0, "pool-b/16C": 71.0})
    B.CFG["relief_cores"] = B.CFG["relief_gi"] = 2.0   # any floor value: slots ignore it
    c, t = B.choose_move(src, "pod", [tgt], rs, world, {"cooldowns": {}, "moves": []},
                         _quota(), set(), {})
    B.CFG["relief_cores"] = B.CFG["relief_gi"] = 0.5
    assert c is not None and c["pod"]["metadata"]["uid"] == pod["metadata"]["uid"]
    assert t.name == "big-tgt"


def test_cpu_dimension_still_honours_the_minimum_benefit_floor():
    """Only slot dimensions skip the floor; a tiny CPU relief is still rejected."""
    src = fill(make_node(name="big-src", cpu=16, mem_gb=64, pods=211, actual_cpu_pct=90), 50)
    pod = _candidate()                               # 0.01 core on 16 cores = 0.06 points
    src.add(pod)
    src.finalize()
    tgt = fill(make_node(name="big-tgt", cpu=16, mem_gb=64, pods=211, actual_cpu_pct=20), 50)
    rs = {pod["metadata"]["ownerReferences"][0]["uid"]: 1}
    B.CFG["relief_cores"] = 0.5

    c, _ = B.choose_move(src, "act_cpu", [tgt], rs, {"big-src": src, "big-tgt": tgt},
                         {"cooldowns": {}, "moves": []}, _quota(), set(), {})
    assert c is None


def test_memory_dimension_uses_absolute_gib_not_percentage_points():
    """RELIEF_GI is a GiB amount, so the same pod is judged the same on any machine size."""
    src = fill(make_node(name="big-src", cpu=16, mem_gb=64, pods=211, actual_mem_pct=92), 60)
    pod = _candidate()
    pod["_act"] = (0.01, 200 * 1024 ** 2)            # 0.2 GiB
    src.add(pod)
    src.finalize()
    tgt = fill(make_node(name="big-tgt", cpu=16, mem_gb=64, pods=211, actual_mem_pct=20), 60)
    rs = {pod["metadata"]["ownerReferences"][0]["uid"]: 1}
    world = {"big-src": src, "big-tgt": tgt}
    B.CFG["relief_gi"] = 0.5

    c, _ = B.choose_move(src, "act_mem", [tgt], rs, world,
                         {"cooldowns": {}, "moves": []}, _quota(), set(), {})
    assert c is None                                 # 0.2 GiB < 0.5 GiB, however small the node
