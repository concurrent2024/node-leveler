"""Cordon handling: the temporary node lock used to steer placement.

Every function here writes to the cluster, so the fake client records the exact
patch bodies. Regressions in this file broke real clusters: a leaked cordon left
nodes unschedulable, and locking the wrong set of nodes sent pods straight back to
their source.
"""
import time

import balancer as B
from conftest import fill, make_node, make_pod


class CordonK8s:
    """Records merge-patches and serves a fixed node list."""

    def __init__(self, nodes):
        self.nodes = nodes
        self.patches = []
        self.code = 200

    def get(self, path):
        assert path == "/api/v1/nodes", path
        return {"items": self.nodes}

    def _req(self, method, path, body, ctype=None):
        self.patches.append({"method": method, "path": path, "body": body})
        return self.code, {}


def node_raw(name, labels=None):
    raw = make_node(name).raw
    if labels:
        raw["metadata"]["labels"].update(labels)
    return raw


# ------------------------------------------------------------- set_cordon

def test_cordon_sets_unschedulable_and_a_timestamp_label():
    k8s = CordonK8s([node_raw("n1")])
    before = int(time.time())

    assert B.set_cordon(k8s, "n1", True) == 200

    body = k8s.patches[0]["body"]
    assert body["spec"]["unschedulable"] is True
    stamp = int(body["metadata"]["labels"][B.CFG["cordon_label"]])
    assert before <= stamp <= int(time.time()) + 1     # a timestamp, not "true"


def test_uncordon_clears_the_label_instead_of_writing_false():
    k8s = CordonK8s([node_raw("n1")])
    B.set_cordon(k8s, "n1", False)

    body = k8s.patches[0]["body"]
    assert body["spec"]["unschedulable"] is False
    assert body["metadata"]["labels"][B.CFG["cordon_label"]] is None


# ------------------------------------------------------------ heal_cordons

def test_heal_releases_only_our_own_cordons():
    k8s = CordonK8s([
        node_raw("ours", {B.CFG["cordon_label"]: str(int(time.time()) - 600)}),
        node_raw("theirs", {"someone-else/cordoned": "true"}),
        node_raw("plain"),
    ])

    healed = B.heal_cordons(k8s)

    assert healed == ["ours"]
    assert [p["path"] for p in k8s.patches] == ["/api/v1/nodes/ours"]


def test_heal_also_picks_up_the_legacy_label_when_configured():
    B.CFG["cordon_label_legacy"] = "old.example/cordoned"
    k8s = CordonK8s([node_raw("legacy", {"old.example/cordoned": str(int(time.time()))})])

    assert B.heal_cordons(k8s) == ["legacy"]


def test_heal_ignores_the_legacy_label_when_it_is_disabled():
    B.CFG["cordon_label_legacy"] = ""
    k8s = CordonK8s([node_raw("legacy", {"old.example/cordoned": "1"})])

    assert B.heal_cordons(k8s) == []


def test_heal_treats_a_malformed_or_legacy_value_as_expired():
    k8s = CordonK8s([node_raw("weird", {B.CFG["cordon_label"]: "true"})])

    assert B.heal_cordons(k8s) == ["weird"]


def test_heal_reports_failure_without_raising():
    class Broken(CordonK8s):
        def get(self, path):
            raise RuntimeError("api down")

    assert B.heal_cordons(Broken([])) == []


# ------------------------------------------------------ cordon_bad_targets

def test_pod_context_locks_every_landable_node_except_the_target():
    src = fill(make_node(name="src", group="pool-a"), 40)
    tgt = fill(make_node(name="tgt", cpu=16, mem_gb=64, pods=211, group="pool-b"), 100)
    other = fill(make_node(name="other", cpu=16, mem_gb=64, pods=211, group="pool-b"), 100)
    scope = {"src": src, "tgt": tgt, "other": other}
    pod = make_pod("app", node="src", node_selector={"node-pool": "pool-a"})
    k8s = CordonK8s([])

    locked = B.cordon_bad_targets(k8s, src, tgt, scope, pod=pod)

    # the pod only matches pool-a, so neither 16C node may be locked; only the source is
    assert locked == ["src"]
    assert "tgt" not in locked


def test_statefulset_locking_stays_inside_the_source_zone():
    src = fill(make_node(name="src", group="pool-a", zone="zone-a"), 40)
    same_zone = fill(make_node(name="same", cpu=16, mem_gb=64, pods=211, group="pool-a", zone="zone-a"), 10)
    other_zone = fill(make_node(name="other", cpu=16, mem_gb=64, pods=211, group="pool-a", zone="zone-b"), 10)
    scope = {"src": src, "same": same_zone, "other": other_zone}
    pod = make_pod("db-0", node="src", owner="StatefulSet")
    k8s = CordonK8s([])

    locked = B.cordon_bad_targets(k8s, src, same_zone, scope, pod=pod, sts=True)

    assert "other" not in locked
    assert "src" in locked


def test_without_pod_context_only_bad_nodes_are_locked():
    B.CFG["cordon_pools"] = ["pool-a/*"]
    src = fill(make_node(name="src", group="pool-a"), 44)
    tgt = fill(make_node(name="tgt", group="pool-a"), 10)
    full = fill(make_node(name="full", group="pool-a"), 44)
    elsewhere = fill(make_node(name="elsewhere", group="pool-b"), 44)
    scope = {"src": src, "tgt": tgt, "full": full, "elsewhere": elsewhere}
    k8s = CordonK8s([])

    locked = B.cordon_bad_targets(k8s, src, tgt, scope)

    assert set(locked) == {"src", "full"}      # elsewhere is outside cordon_pools
    assert "tgt" not in locked


def test_uncordon_all_releases_every_name():
    k8s = CordonK8s([])
    B.uncordon_all(k8s, ["a", "b"])

    assert [p["path"] for p in k8s.patches] == ["/api/v1/nodes/a", "/api/v1/nodes/b"]
    assert all(p["body"]["spec"]["unschedulable"] is False for p in k8s.patches)
