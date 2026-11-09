"""Waiting for a replacement pod.

The subtle bug these tests exist for: a ReplicaSet with more than one replica always
has pods that are not the replacement. Accepting one of those made the balancer think
a move had finished when it had not, and recorded a real move as a bounce.
"""
import time

import balancer as B
from conftest import make_pod


class SettleK8s:
    """Serves a scripted sequence of pod lists (one list per poll)."""

    def __init__(self, pages, single=None):
        self.pages = list(pages)
        self.single = single
        self.polls = 0

    def get(self, path):
        self.polls += 1
        if "/pods/" in path:                       # StatefulSet path: one pod
            if self.single is None:
                raise RuntimeError("not found")
            return self.single
        return {"items": self.pages.pop(0) if self.pages else []}


def pod(name, node, *, uid="new-uid", age_offset=0, phase="Running", ready=None,
        waiting=None, rsuid="rs-1"):
    p = make_pod(name, node=node, uid=uid)
    p["metadata"]["ownerReferences"][0]["uid"] = rsuid
    p["metadata"]["creationTimestamp"] = time.strftime(
        "%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + age_offset))
    if ready is None:
        ready = waiting is None          # a waiting container is never ready
    p["status"]["phase"] = phase
    statuses = [{"ready": ready}]
    if waiting:
        statuses[0]["state"] = {"waiting": {"reason": waiting}}
    p["status"]["containerStatuses"] = statuses
    return p


def no_sleep(monkeypatch):
    monkeypatch.setattr(B.time, "sleep", lambda *_: None)


# ---------------------------------------------------------------- Deployment

def test_returns_ok_for_a_ready_replacement(monkeypatch):
    no_sleep(monkeypatch)
    k8s = SettleK8s([[pod("app-new", "node-b")]])

    assert B.wait_settle(k8s, "app1", "rs-1", "old-uid", "node-a", timeout=5) == \
        ("ok", "node-b", "app-new")


def test_ignores_a_sibling_replica_created_before_the_eviction(monkeypatch):
    """Regression: the sibling of a multi-replica Deployment is not the replacement."""
    no_sleep(monkeypatch)
    sibling = pod("app-sibling", "node-a", uid="sibling-uid", age_offset=-600)
    real = pod("app-new", "node-c")
    k8s = SettleK8s([[sibling], [sibling, real]])

    status, node, name = B.wait_settle(k8s, "app1", "rs-1", "old-uid", "node-a",
                                       timeout=5, since_ts=time.time() - 30)

    assert (status, node, name) == ("ok", "node-c", "app-new")
    assert k8s.polls == 2


def test_ignores_the_evicted_pod_itself(monkeypatch):
    no_sleep(monkeypatch)
    same = pod("app-old", "node-a", uid="old-uid")
    k8s = SettleK8s([[same], []])

    assert B.wait_settle(k8s, "app1", "rs-1", "old-uid", "node-a", timeout=0)[0] == "timeout"


def test_ignores_pods_of_other_workloads(monkeypatch):
    no_sleep(monkeypatch)
    foreign = pod("other", "node-b", rsuid="rs-other")
    k8s = SettleK8s([[foreign]])

    assert B.wait_settle(k8s, "app1", "rs-1", "old-uid", "node-a", timeout=0)[0] == "timeout"


def test_reports_a_failed_container_immediately(monkeypatch):
    no_sleep(monkeypatch)
    k8s = SettleK8s([[pod("app-new", "node-b", waiting="CrashLoopBackOff")]])

    assert B.wait_settle(k8s, "app1", "rs-1", "old-uid", "node-a", timeout=5) == \
        ("CrashLoopBackOff", "node-b", "app-new")


def test_image_pull_failure_is_reported_too(monkeypatch):
    no_sleep(monkeypatch)
    k8s = SettleK8s([[pod("app-new", "node-b", waiting="ImagePullBackOff")]])

    assert B.wait_settle(k8s, "app1", "rs-1", "old-uid", "node-a", timeout=5)[0] == "ImagePullBackOff"


def test_a_running_but_unready_pod_is_not_accepted(monkeypatch):
    no_sleep(monkeypatch)
    k8s = SettleK8s([[pod("app-new", "node-b", ready=False)]])

    assert B.wait_settle(k8s, "app1", "rs-1", "old-uid", "node-a", timeout=0)[0] == "timeout"


def test_api_errors_do_not_escape(monkeypatch):
    no_sleep(monkeypatch)

    class Flaky(SettleK8s):
        def get(self, path):
            raise RuntimeError("apiserver unavailable")

    assert B.wait_settle(Flaky([]), "app1", "rs-1", "old-uid", "node-a", timeout=0)[0] == "timeout"


# -------------------------------------------------------------- StatefulSet

def test_statefulset_settles_on_a_new_uid(monkeypatch):
    no_sleep(monkeypatch)
    k8s = SettleK8s([], single=pod("db-0", "node-b", uid="new-uid"))

    assert B.wait_settle_sts(k8s, "app1", "db-0", "old-uid", "node-a", timeout=5) == \
        ("ok", "node-b", "db-0")


def test_statefulset_does_not_settle_on_the_old_uid(monkeypatch):
    no_sleep(monkeypatch)
    k8s = SettleK8s([], single=pod("db-0", "node-a", uid="old-uid"))

    assert B.wait_settle_sts(k8s, "app1", "db-0", "old-uid", "node-a", timeout=0)[0] == "timeout"


def test_statefulset_reports_a_failed_container(monkeypatch):
    no_sleep(monkeypatch)
    k8s = SettleK8s([], single=pod("db-0", "node-b", waiting="ImagePullBackOff"))

    assert B.wait_settle_sts(k8s, "app1", "db-0", "old-uid", "node-a", timeout=5)[0] == "ImagePullBackOff"
