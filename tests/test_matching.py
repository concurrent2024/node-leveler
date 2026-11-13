"""Placement matching and the post-move landing check."""
import balancer as B
from conftest import fill, make_node, make_pod


def node_with_taints(taints, **kw):
    node = fill(make_node(**kw), 5)
    node.raw["spec"]["taints"] = taints
    return node


# -------------------------------------------------------------- tolerations

def test_an_untainted_node_accepts_everything():
    assert B.tolerations_ok(make_pod("p"), node_with_taints([])) is True


def test_a_matching_key_value_toleration_is_accepted():
    node = node_with_taints([{"key": "group", "value": "data", "effect": "NoSchedule"}])
    pod = make_pod("p", tolerations=[{"key": "group", "value": "data", "effect": "NoSchedule"}])

    assert B.tolerations_ok(pod, node) is True


def test_a_different_value_is_rejected():
    node = node_with_taints([{"key": "group", "value": "data", "effect": "NoSchedule"}])
    pod = make_pod("p", tolerations=[{"key": "group", "value": "other", "effect": "NoSchedule"}])

    assert B.tolerations_ok(pod, node) is False


def test_exists_without_a_key_tolerates_everything():
    node = node_with_taints([{"key": "group", "value": "data", "effect": "NoSchedule"}])
    pod = make_pod("p", tolerations=[{"operator": "Exists"}])

    assert B.tolerations_ok(pod, node) is True


def test_prefer_no_schedule_taints_are_not_a_hard_gate():
    node = node_with_taints([{"key": "group", "value": "data", "effect": "PreferNoSchedule"}])

    assert B.tolerations_ok(make_pod("p"), node) is True


def test_every_taint_has_to_be_tolerated():
    node = node_with_taints([
        {"key": "group", "value": "data", "effect": "NoSchedule"},
        {"key": "dedicated", "value": "db", "effect": "NoExecute"},
    ])
    pod = make_pod("p", tolerations=[{"key": "group", "value": "data", "effect": "NoSchedule"}])

    assert B.tolerations_ok(pod, node) is False


# ---------------------------------------------------------------- node selector / affinity

def test_node_selector_must_match():
    raw = make_node(labels={"node-pool": "pool-a"}).raw
    assert B.node_selector_ok(make_pod("p", node_selector={"node-pool": "pool-a"}), raw) is True
    assert B.node_selector_ok(make_pod("p", node_selector={"node-pool": "pool-b"}), raw) is False


def test_required_node_affinity_in_operator():
    raw = make_node(labels={"zone": "z1"}).raw
    pod = make_pod("p")
    pod["spec"]["affinity"] = {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
        "nodeSelectorTerms": [{"matchExpressions": [
            {"key": "zone", "operator": "In", "values": ["z1", "z2"]}]}]}}}

    assert B.required_node_affinity_ok(pod, raw) is True

    pod["spec"]["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"][
        "nodeSelectorTerms"][0]["matchExpressions"][0]["values"] = ["z9"]
    assert B.required_node_affinity_ok(pod, raw) is False


def test_required_node_affinity_exists_and_does_not_exist():
    raw = make_node(labels={"gpu": "true"}).raw
    pod = make_pod("p")
    term = {"matchExpressions": [{"key": "gpu", "operator": "Exists"}]}
    pod["spec"]["affinity"] = {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
        "nodeSelectorTerms": [term]}}}
    assert B.required_node_affinity_ok(pod, raw) is True

    term["matchExpressions"][0] = {"key": "missing", "operator": "DoesNotExist"}
    assert B.required_node_affinity_ok(pod, raw) is True


# --------------------------------------------------------- landing guard

def test_landing_guard_accepts_a_healthy_node():
    assert B.landing_guard_violation({"pod_pct": 80, "memuse_pct": 70, "cpu_pct": 50}) is None


def test_landing_guard_flags_full_slots():
    assert B.landing_guard_violation({"pod_pct": 97, "memuse_pct": 10, "cpu_pct": 10}) == "pod_slots"


def test_landing_guard_flags_high_real_memory():
    assert B.landing_guard_violation({"pod_pct": 50, "memuse_pct": 95, "cpu_pct": 10}) == "mem_actual"


def test_landing_guard_flags_cpu():
    assert B.landing_guard_violation({"pod_pct": 50, "memuse_pct": 50, "cpu_pct": 95}) == "cpu"


def test_landing_guard_ignores_missing_measurements():
    assert B.landing_guard_violation(None) is None
