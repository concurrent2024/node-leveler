"""Building the world from the API: pagination, slimming, scope and Pending counting."""
import balancer as B
from conftest import make_node, make_pod


def node_obj(name, cpu=4, pods=46, mem_gb=16, group="pool-a"):
    raw, metrics = make_node(name, cpu=cpu, pods=pods, mem_gb=mem_gb, group=group), None
    return raw.raw


class FakeK8s:
    """Minimal stand-in for the API client: only the calls build_world() makes."""

    def __init__(self, nodes, pods, node_metrics=None, pod_metrics=None, page_size=2):
        self._nodes = nodes
        self._pods = pods
        self._node_metrics = node_metrics or []
        self._pod_metrics = pod_metrics or []
        self.page_size = page_size
        self.calls = []

    def get(self, path):
        self.calls.append(path)
        if path.startswith("/api/v1/nodes"):
            return {"items": self._nodes}
        if path.startswith("/api/v1/pods"):
            # emulate the API server's continue-token pagination
            cont = 0
            for param in path.split("&"):
                if param.startswith("continue="):
                    cont = int(param.split("=", 1)[1])
            page = self._pods[cont:cont + self.page_size]
            nxt = cont + self.page_size
            meta = {"continue": str(nxt)} if nxt < len(self._pods) else {}
            return {"items": page, "metadata": meta}
        raise AssertionError(f"unexpected GET {path}")

    def try_get(self, path):
        if path.endswith("/v1beta1/nodes"):
            return {"items": self._node_metrics}
        if path.endswith("/v1beta1/pods"):
            return {"items": self._pod_metrics}
        return None


def _pod(name, node, ns="app1", phase="Running", age_ok=True):
    p = make_pod(name, ns=ns, node=node, age_min=60)
    if phase != "Running":
        p["status"]["phase"] = phase
        if phase == "Pending":
            p["spec"]["nodeName"] = None
    return p


def test_pagination_collects_every_page():
    nodes = [node_obj("n1"), node_obj("n2")]
    pods = [_pod(f"p{i}", "n1" if i % 2 else "n2") for i in range(5)]
    k8s = FakeK8s(nodes, pods, page_size=2)

    world, _ = B.build_world(k8s)

    assert sum(len(n.pods) for n in world.values()) == 5
    assert sum(1 for c in k8s.calls if c.startswith("/api/v1/pods")) == 3   # 2 + 2 + 1


def test_slim_pod_keeps_only_decision_fields():
    full = _pod("p1", "n1")
    full["metadata"]["labels"] = {"x": "y"}          # noise that must be dropped
    full["metadata"]["managedFields"] = [{"x": 1}]
    slim = B.slim_pod(full)

    assert "labels" not in slim["metadata"] and "managedFields" not in slim["metadata"]
    for field in ("name", "namespace", "uid", "ownerReferences", "creationTimestamp"):
        assert field in slim["metadata"]
    assert "containers" in slim["spec"] and "nodeName" in slim["spec"]


def test_only_running_pods_that_sit_on_known_nodes_are_counted():
    nodes = [node_obj("n1")]
    pods = [_pod("ok", "n1"), _pod("gone", "n1", phase="Succeeded")]
    world, _ = B.build_world(FakeK8s(nodes, pods))
    assert len(world["n1"].pods) == 1


def test_pending_pods_are_counted_for_the_pressure_gate():
    nodes = [node_obj("n1")]
    pods = [_pod("stuck", "n1", phase="Pending")]
    B.build_world(FakeK8s(nodes, pods))
    assert B.PENDING["n"] == 1


def test_stale_pending_pods_do_not_trigger_the_pressure_gate():
    """A pod that has been unschedulable for weeks is a config bug, not capacity pressure."""
    nodes = [node_obj("n1")]
    stale = _pod("old-stuck", "n1", phase="Pending")
    stale["metadata"]["creationTimestamp"] = "2026-01-01T00:00:00Z"
    B.build_world(FakeK8s(nodes, [stale]))
    assert B.PENDING["n"] == 0


def test_replica_counts_drive_the_multi_replica_preference():
    nodes = [node_obj("n1"), node_obj("n2")]
    a1 = _pod("a1", "n1", ns="app1")
    a2 = _pod("a2", "n2", ns="app1")
    for p in (a1, a2):                       # same ReplicaSet -> 2 replicas
        p["metadata"]["ownerReferences"][0]["uid"] = "rs-a"
    solo = _pod("s1", "n1", ns="app1")
    solo["metadata"]["ownerReferences"][0]["uid"] = "rs-solo"
    _, rs = B.build_world(FakeK8s(nodes, [a1, a2, solo]))
    assert rs["rs-a"] == 2 and rs["rs-solo"] == 1


def test_in_scope_filters_by_configured_pools():
    B.CFG["pools"] = {"pool-a/4C"}
    nodes = {n.name: n for n in [make_node("small", cpu=4), make_node("big", cpu=16)]}
    for n in nodes.values():
        n.finalize()
    assert set(B.in_scope(nodes)) == {"small"}
