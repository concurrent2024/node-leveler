"""Shared fixtures.

The balancer keeps its configuration in a module-level dict (``CFG``) built from
environment variables at import time, and a couple of module-level caches. Tests
mutate ``CFG`` directly and the fixtures below restore everything afterwards, so
each test starts from the documented defaults.
"""
import copy
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import balancer as B  # noqa: E402  (import after sys.path tweak)


GB = 1024 ** 3


def _ts(minutes_ago: int) -> str:
    t = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


# Thresholds are shipped in chart/values.yaml, not in the code defaults. Tests run
# against the shipped configuration so they describe production behaviour.
# Snapshot of the code defaults taken before any test mutates CFG.
CODE_DEFAULTS = copy.deepcopy(B.CFG)

PROD_DEFAULTS = {
    "pod_high": 95.0, "target_pod_max": 90.0, "target_cpu_max": 85.0,
    "target_mem_max": 80.0, "target_memuse_max": 90.0, "target_memuse_basis": "physical",
    "phys_after_mem_max": 85.0, "target_after_cpu_max": 80.0, "target_after_mem_max": 85.0,
    "mem_basis": "physical", "mem_act_high": 85.0, "cpu_act_high": 60.0, "mem_critical": 90.0,
    "pool_slot_pressure": 85.0, "pool_pressure_src_min": 85.0, "pool_slot_notify": 85.0,
    "single_improve_margin": 5.0, "improve_margin": 0.0, "act_improve_margin": 5.0,
    "relief_cores": 0.5, "relief_gi": 0.5, "act_missing_policy": "skip", "balance_dim": "actual",
    "pool_label": "node-pool", "ns_include": r"^(app|batch)\d+$", "pools": {"pool-a/4C", "pool-b/16C"},
}


@pytest.fixture(autouse=True)
def prod_config():
    B.CFG.update(PROD_DEFAULTS)
    yield


@pytest.fixture(autouse=True)
def clean_state():
    """Restore CFG and module-level caches after every test."""
    cfg = copy.deepcopy(B.CFG)
    pool_used = dict(B.POOL_USED)
    round_sources = set(B.ROUND_SOURCES)
    pending = dict(B.PENDING)
    yield
    B.CFG.clear()
    B.CFG.update(cfg)
    B.POOL_USED.clear()
    B.POOL_USED.update(pool_used)
    B.ROUND_SOURCES.clear()
    B.ROUND_SOURCES.update(round_sources)
    B.PENDING.clear()
    B.PENDING.update(pending)


def make_node(
    name="node-a",
    *,
    cpu=4,
    mem_gb=16,
    pods=46,
    group="pool-a",
    used_pods=0,
    actual_mem_pct=0.0,
    actual_cpu_pct=0.0,
    zone="zone-a",
    unschedulable=False,
    ready=True,
    pressure=False,
    labels=None,
):
    """A node as the Kubernetes API would return it, plus its metrics usage."""
    cap = mem_gb * GB
    raw = {
        "metadata": {
            "name": name,
            "labels": {**(labels or {}), "node-pool": group, "topology.kubernetes.io/zone": zone},
        },
        "spec": {"unschedulable": unschedulable} if unschedulable else {},
        "status": {
            "capacity": {"cpu": str(cpu), "memory": f"{mem_gb}Gi", "pods": str(pods)},
            # allocatable is deliberately ~90% of capacity, as on real managed node groups
            "allocatable": {"cpu": str(cpu), "memory": f"{int(mem_gb * 0.9)}Gi", "pods": str(pods)},
            "conditions": [{"type": "Ready", "status": "True" if ready else "False"}]
            + ([{"type": "MemoryPressure", "status": "True"}] if pressure else []),
        },
    }
    metrics = {
        name: {
            "metadata": {"name": name},
            "usage": {
                "cpu": f"{cpu * actual_cpu_pct / 100:.3f}",
                "memory": f"{int(cap * actual_mem_pct / 100)}",
            },
        }
    }
    return B.Node(raw, metrics)


def make_pod(
    name="app1",
    *,
    ns="app1",
    node="node-a",
    cpu=0.1,
    mem_mb=128,
    owner="ReplicaSet",
    owner_name=None,
    owner_uid=None,
    uid=None,
    phase="Running",
    age_min=60,
    restarts=0,
    ready=True,
    node_selector=None,
    tolerations=None,
):
    owner_uid = owner_uid or f"uid-{owner_name or name}"
    return {
        "metadata": {
            "name": name,
            "namespace": ns,
            "uid": uid or f"pod-uid-{ns}-{name}",
            "creationTimestamp": _ts(age_min),
            "ownerReferences": [{"kind": owner, "name": owner_name or name, "uid": owner_uid}],
        },
        "spec": {
            "nodeName": node,
            "containers": [
                {"name": "c", "resources": {"requests": {"cpu": str(cpu), "memory": f"{mem_mb}Mi"}}}
            ],
            **({"nodeSelector": node_selector} if node_selector else {}),
            **({"tolerations": tolerations} if tolerations else {}),
        },
        "status": {"phase": phase, "containerStatuses": [{"restartCount": restarts, "ready": ready}]},
    }


def fill(node, count, **pod_kw):
    """Attach `count` filler pods to a node (they only consume slots)."""
    pod_kw.setdefault("cpu", 0.01)
    pod_kw.setdefault("mem_mb", 64)
    for i in range(count):
        node.add(make_pod(f"filler-{i}", node=node.name, **pod_kw))
    node.finalize()
    return node


def set_pool_usage(**pools):
    """Populate the pool-utilisation cache used by the pool-pressure driver."""
    B.POOL_USED.clear()
    B.POOL_USED.update(pools)
