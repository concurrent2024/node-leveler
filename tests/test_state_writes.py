"""State ConfigMap writes across the first-round boundary.

A fresh install has no state ConfigMap, so the first write is a POST. Every later write
must notice the object now exists and switch to PUT — otherwise it POSTs again and the
API server answers 409 AlreadyExists, which the caller only sees as a non-200 status code.
On a real cluster that meant the single-instance lock was never released and no move was
ever recorded, while the round itself still reported success.
"""
import balancer as B


class StateK8s:
    def __init__(self, exists_from_start=False):
        self.present = exists_from_start
        self.calls = []
        self.requests = []

    def try_get(self, path):
        if not self.present:
            return None
        return {"metadata": {"name": "node-leveler-state", "namespace": "cluster-ops",
                             "resourceVersion": "42"}, "data": {"state": "{}"}}

    def _req(self, method, path, body, ctype=None):
        self.calls.append(method)
        self.requests.append((method, body))
        if method == "POST":
            self.present = True          # the POST creates the object
            return 201, {}
        return 200, {}


def _state():
    return {"cooldowns": {}, "moves": [], "ema": {"nodes": {}}}


def test_first_write_creates_then_later_writes_update():
    B.CFG["state_cm"] = ("cluster-ops", "node-leveler-state")
    k8s = StateK8s(exists_from_start=False)

    assert B.save_state(k8s, _state(), None, {"mode": "dry-run", "locked": True}) == 201
    assert B.save_state(k8s, _state(), None, {"mode": "dry-run", "moves": 0}) == 200
    assert k8s.calls == ["POST", "PUT"]


def test_update_carries_the_current_resource_version():
    B.CFG["state_cm"] = ("cluster-ops", "node-leveler-state")
    k8s = StateK8s(exists_from_start=True)

    assert B.save_state(k8s, _state(), None, {"mode": "live"}) == 200
    method, body = k8s.requests[-1]
    assert method == "PUT" and body["metadata"]["resourceVersion"] == "42"


def test_existing_object_is_never_posted_again():
    B.CFG["state_cm"] = ("cluster-ops", "node-leveler-state")
    k8s = StateK8s(exists_from_start=True)
    for _ in range(3):
        B.save_state(k8s, _state(), None, {"mode": "live"})
    assert k8s.calls == ["PUT", "PUT", "PUT"]
