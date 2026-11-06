#!/usr/bin/env python3
"""
node-leveler — continuous node waterline balancing (in-cluster CronJob or local dry-run)

Design principles
  1. Two jobs only: read waterlines, pick victim pods from the allow-list and evict them.
     Never delete pods, never touch workloads, never patch nodes.
  2. Placement is left to kube-scheduler. This script only guarantees that the source is
     genuinely overloaded and that the target can actually hold the pod.
  3. Every write action is off by default: MODE=dry-run only prints a plan. Going live
     requires an explicit ConfigMap change.
  4. Throttles at node, namespace and cycle level, plus a per-workload cooldown
     (anti-flapping) and a daily budget.
  5. Post-check: a moved pod must come up Running off the source node. Landing back on the
     source counts as a bounce and cools the workload down.

Usage
  in-cluster : python3 balancer.py            # service-account token + env from the ConfigMap
  local      : KUBECONFIG=~/.kube/<cluster> python3 balancer.py --dry-run
"""
import fnmatch
import json
import atexit
import signal
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

# ---------------------------------------------------------------- config

def _env(name, default):
    v = os.environ.get(name)
    return default if v is None or v == "" else v

def own_namespace():
    """The namespace this process runs in.

    Used as the default location for the parameter and state ConfigMaps, so a
    deployment never has to hard-code a namespace: install it anywhere and it keeps
    its state there. In-cluster the service account exposes it; for local runs
    POD_NAMESPACE or NAMESPACE can be set, otherwise "default" is used.
    """
    for key in ("POD_NAMESPACE", "NAMESPACE"):
        value = os.environ.get(key)
        if value:
            return value.strip()
    try:
        with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as fh:
            return fh.read().strip() or "default"
    except OSError:
        return "default"



def _cm_ref(env_key, default_name):
    """Resolve a ConfigMap reference from "<ns>/<name>"; default namespace = our own."""
    raw = _env(env_key, "").strip()
    if not raw:
        return (own_namespace(), default_name)
    parts = raw.split("/")
    if len(parts) == 1:
        return (own_namespace(), parts[0])
    return (parts[0], parts[1])

CFG = {
    "mode": _env("MODE", "dry-run"),                 # dry-run | live | off
    # balancing dimensions: pod slots first, then real CPU/memory
    "balance_dim": _env("BALANCE_DIM", "actual"),    # actual | requests | both
    "cpu_act_high": float(_env("CPU_ACT_HIGH", "60")),
    "mem_act_high": float(_env("MEM_ACT_HIGH", "85")),
    # memory basis for source/hotspot detection:
#   alloc    = share of allocatable (legacy)
#   physical = share of capacity (closer to OOM risk)
    "mem_basis": _env("MEM_BASIS", "physical"),
    # Targets additionally get a physical-memory hard cap: 90% of allocatable is only 73%
    # of physical on a small node but 84% on a node with 14.1G allocatable, which is close
# to the danger line. This gate always uses the physical percentage.
    "phys_after_mem_max": float(_env("PHYS_AFTER_MEM_MAX", "85")),
    "act_alpha": float(_env("ACT_ALPHA", "0.35")),        # EMA smoothing factor
    "act_min_samples": int(_env("ACT_MIN_SAMPLES", "3")), # do not draw conclusions below this many samples
    "act_after_cpu_max": float(_env("ACT_AFTER_CPU_MAX", "65")),
    "act_after_mem_max": float(_env("ACT_AFTER_MEM_MAX", "90")),
    "mem_critical": float(_env("MEM_CRITICAL", "90")),   # real memory above this value jumps the queue, ahead of slot pressure
    # pods without metrics samples: skip = not moved this round (default; unknown is not
#   zero) | zero = treated as 0 (legacy, can let memory-heavy pods through)
    "act_missing_policy": _env("ACT_MISSING_POLICY", "skip"),
    # StatefulSets (single-replica PG/Redis/RabbitMQ) are untouched by default; ALLOW_STS_NS
# opens matching namespaces,
    # and the target must sit in the same zone as the source: block-storage PVs carry a
# zone-level nodeAffinity, so cross-zone moves stay Pending.
    "allow_sts_ns": _env("ALLOW_STS_NS", ""),
    "max_sts_per_cycle": int(_env("MAX_STS_PER_CYCLE", "0")),
    "sts_cooldown_hours": float(_env("STS_COOLDOWN_HOURS", "6")),
    # StatefulSet rebuilds are slow: volume attach + startup + cluster join often exceed
    # five minutes. A 300s timeout was once misread as a failure, which aborted the round and
# froze the balancer in dry-run.
    "sts_settle_timeout": int(_env("STS_SETTLE_TIMEOUT", "300")),
    # The improvement margin for StatefulSets is 0: a few-hundred-MiB pod moving from a
# small node to a large one
    # shifts the target by less than one point, so the 5-point Deployment margin vetoed
# every such move.
    "sts_improve_margin": float(_env("STS_IMPROVE_MARGIN", "0")),
    "act_improve_margin": float(_env("ACT_IMPROVE_MARGIN", "5")),
    # pool = pool label + CPU size class: nodes of different shapes in one group must never
# be mixed into one percentage.
    "pools": [x.strip() for x in _env("POOLS", "").split(",") if x.strip()],
    # empty by default: nothing is scanned until the operator names their pools
    # Cordon guidance only locks pools a business pod could really land on; tainted pools
    # are never chosen by the scheduler, so locking them would only widen the unschedulable
# window for no benefit.
    "cordon_pools": [x.strip() for x in _env("CORDON_POOLS", "").split(",") if x.strip()],
    "cordon_ttl_min": int(_env("CORDON_TTL_MIN", "10")),
    # source waterlines (absolute values; any hit means overloaded)
    "cpu_high": float(_env("CPU_HIGH", "75")),
    "mem_high": float(_env("MEM_HIGH", "80")),
    "pod_high": float(_env("POD_HIGH", "95")),
    # target ceilings
    "target_pod_max": float(_env("TARGET_POD_MAX", "90")),
    "target_mem_max": float(_env("TARGET_MEM_MAX", "70")),
    "target_memuse_max": float(_env("TARGET_MEMUSE_MAX", "90")),
    # denominator for the target memory guard: alloc (legacy) / physical (recommended)
    # allocatable is usually ~90% of capacity, so 80% of physical reads as 89% of allocatable
# and keeps nodes with tens of GiB free out of the candidate set.
    "target_memuse_basis": _env("TARGET_MEMUSE_BASIS", "physical"),
    # Third gate: the outcome. A fixed "target must be N points lower" rule never fires in a
# cluster whose waterlines all sit between 50 and 77%.
    # Required instead: (1) the source must genuinely improve (RELIEF_CORES/RELIEF_GI);
# (2) the target must stay under its ceilings (TARGET_AFTER_*);
    # (3) the target must end up better than the source would be (IMPROVE_MARGIN, stricter for
# single replicas). All three, otherwise it flaps.
    # minimum benefit in absolute resource units, so it never depends on machine size;
    # slot dimensions do not consult these keys at all
    "relief_cores": float(_env("RELIEF_CORES", "0.5")),
    "relief_gi": float(_env("RELIEF_GI", "0.5")),
    "improve_margin": float(_env("IMPROVE_MARGIN", "0")),
    "single_improve_margin": float(_env("SINGLE_IMPROVE_MARGIN", "5")),
    "landing_guard": _env("LANDING_GUARD", "true").lower() == "true",
    # throttles
    "max_per_cycle": int(_env("MAX_PER_CYCLE", "4")),
    "max_per_node": int(_env("MAX_PER_NODE", "4")),
    "max_per_ns": int(_env("MAX_PER_NS", "2")),
    "max_single_replica": int(_env("MAX_SINGLE_REPLICA", "0")),
    "daily_budget": int(_env("DAILY_BUDGET", "100")),
    "max_pod_cpu": float(_env("MAX_POD_CPU", "2")),      # a pod this large is not worth the risk
    "max_pod_mem_gi": float(_env("MAX_POD_MEM_GI", "4")),
    # pod filters (all configurable from the ConfigMap; no code changes needed)
    # "^$" matches nothing: namespaces must be opted in explicitly
    "ns_include": _env("NS_INCLUDE", r"^$"),
    "ns_exclude": set(filter(None, _env("NS_EXCLUDE", "").split(","))),
    # workload lists: "<ns>/<workload>" globs with * and ?; deny wins over allow
    "deny_workloads": [x.strip() for x in _env("DENY_WORKLOADS", "").split(",") if x.strip()],
    "allow_workloads": [x.strip() for x in _env("ALLOW_WORKLOADS", "").split(",") if x.strip()],
    # permissive switches (all false by default: the most conservative setting)
    "allow_statefulset": _env("ALLOW_STATEFULSET", "false").lower() == "true",
    "allow_storage": _env("ALLOW_STORAGE", "false").lower() == "true",
    "allow_hostnetwork": _env("ALLOW_HOSTNETWORK", "false").lower() == "true",
    "allow_not_ready": _env("ALLOW_NOT_READY", "false").lower() == "true",
    "allow_required_antiaffinity": _env("ALLOW_REQUIRED_ANTIAFFINITY", "false").lower() == "true",
    # cross-pool (cross machine-shape) moves are allowed both ways: the pod's own placement
# rules decide, not the pool name
    "allow_cross_pool": _env("ALLOW_CROSS_POOL", "true").lower() == "true",
    "target_cpu_max": float(_env("TARGET_CPU_MAX", "75")),
    # landing guard: a target must not become the new hotspot
    "target_after_cpu_max": float(_env("TARGET_AFTER_CPU_MAX", "80")),
    "target_after_mem_max": float(_env("TARGET_AFTER_MEM_MAX", "85")),
    # ordering strategy
    "prefer_multi_replica": _env("PREFER_MULTI_REPLICA", "true").lower() == "true",
    "relief_dim": _env("RELIEF_DIM", "auto"),          # auto | cpu | mem | pod
    "min_age_min": int(_env("MIN_POD_AGE_MIN", "20")),
    "max_restarts": int(_env("MAX_RESTARTS", "3")),
    "cooldown_hours": float(_env("COOLDOWN_HOURS", "6")),
    # pool saturation: when slots are nearly exhausted this is a capacity problem, not a
# distribution problem
    "pool_min_free_pct": float(_env("POOL_MIN_FREE_PCT", "2")),
    # Capacity-pressure fallback: hotspots are normally judged on physical memory, but if
    # pods are stuck Pending and a node's alloc usage is at or above ALLOC_PRESSURE_HIGH,
    # treat it as hot anyway (otherwise a node full on requests is never picked and new pods
# simply queue).
    "pending_pressure": _env("PENDING_PRESSURE", "true").lower() == "true",
    "alloc_pressure_high": float(_env("ALLOC_PRESSURE_HIGH", "98")),
    "pending_min_age_min": float(_env("PENDING_MIN_AGE_MIN", "2")),
    # pool-level slot alert: a single node at 95% is not urgent, a whole pool above the line is
    "pool_slot_notify": float(_env("POOL_SLOT_NOTIFY", "85")),
    # Pool-pressure driver: once a pool crosses the slot line it is drained rather than only
    # alerted on; nodes above the per-node floor become sources. 0 disables the driver.
    "pool_slot_pressure": float(_env("POOL_SLOT_PRESSURE", "85")),
    "pool_pressure_src_min": float(_env("POOL_PRESSURE_SRC_MIN", "85")),
    # state. Default: our own namespace, so nothing has to be configured per install.
    "state_cm": _cm_ref("STATE_CM", "node-leveler-state"),
    # own parameter ConfigMap (auto-downgrade writes MODE=dry-run here)
    "cfg_cm": _cm_ref("CFG_CM", "node-leveler-config"),
    "auto_downgrade": _env("AUTO_DOWNGRADE", "true").lower() == "true",
    # Landing guidance: briefly cordon every node the pod could land on except the chosen
# target, then uncordon as soon as the replacement is running
    # Cordons left behind by a crash are healed at the start of each round (label value is a
# timestamp)
    "cordon_guide": _env("CORDON_GUIDE", "true").lower() == "true",
    # No cordon guidance for StatefulSets by default: with PV zone affinity the candidate set
    # is already tiny, and locking the same-zone alternatives leaves the pod Pending when the
# target cannot take it.
    "cordon_guide_sts": _env("CORDON_GUIDE_STS", "false").lower() == "true",
    "cordon_label": _env("CORDON_LABEL", "node-leveler/cordoned"),
    # legacy label (migration aid): empty by default; older deployments point it at their
# historical label
    "cordon_label_legacy": _env("CORDON_LABEL_LEGACY", ""),
    "pool_label": _env("POOL_LABEL", "node-pool"),
    # DingTalk notifications (token comes from a Secret, never from this file or the ConfigMap)
    "dingtalk_token": _env("DINGTALK_TOKEN", ""),
    "dingtalk_secret": _env("DINGTALK_SECRET", ""),
    "notify_enabled": _env("NOTIFY_ENABLED", "true").lower() == "true",
    "notify_min_interval_min": int(_env("NOTIFY_MIN_INTERVAL_MIN", "180")),
    # cluster name shown in notifications (no real cluster name is hard-coded)
    "cluster_name": _env("CLUSTER_NAME", "cluster"),
    "notify_title": _env("NOTIFY_TITLE", "node-leveler"),
    "settle_timeout": int(_env("SETTLE_TIMEOUT", "300")),
}

# pods skipped this round for lack of metrics samples (counted in the log only)
NO_METRICS = {"n": 0}
# unschedulable Pending pods (capacity-pressure gate): no nodeName and pending longer
# than the configured age
PENDING = {"n": 0}
# nodes that are sources this round: excluded as targets so a node is not drained and
# refilled in the same round
ROUND_SOURCES = set()
# slot utilisation per pool this round (%): the pool-pressure driver reads this
POOL_USED = {}





def refresh_pool_used(scope):
    """Recompute per-pool slot utilisation for the current scope (live rebuilds the world after
    every move, so this has to be refreshed each time)."""
    agg = {}
    for n in scope.values():
        a = agg.setdefault(n.pool_name, [0, 0])
        a[0] += len(n.pods)
        a[1] += n.alloc_pods
    POOL_USED.clear()
    for pool, (used, cap) in agg.items():
        POOL_USED[pool] = used / cap * 100 if cap else 0.0

SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"

# ---------------------------------------------------------------- k8s client

class K8s:
    def __init__(self):
        if os.path.exists(f"{SA_DIR}/token"):
            self.host = "https://" + os.environ["KUBERNETES_SERVICE_HOST"] + ":" + os.environ["KUBERNETES_SERVICE_PORT"]
            self.ctx = ssl.create_default_context(cafile=f"{SA_DIR}/ca.crt")
            self._token = open(f"{SA_DIR}/token").read().strip()
        else:  # local dry-run: borrow the kubeconfig from kubectl (inline credentials supported)
            import base64
            import subprocess
            import tempfile
            raw = subprocess.run(["kubectl", "config", "view", "--raw", "-o", "json", "--minify"],
                                 capture_output=True, text=True, check=True).stdout
            kc = json.loads(raw)
            c = kc["clusters"][0]["cluster"]
            self.host = c["server"].rstrip("/")
            u = kc["users"][0]["user"]
            if c.get("insecure-skip-tls-verify"):
                self.ctx = ssl._create_unverified_context()
            elif c.get("certificate-authority-data"):
                self.ctx = ssl.create_default_context(
                    cadata=base64.b64decode(c["certificate-authority-data"]).decode())
            else:
                self.ctx = ssl.create_default_context(cafile=c.get("certificate-authority"))
            self._token = u.get("token")

            def _tmp(kind, data_b64):
                fd, path = tempfile.mkstemp(prefix=f"kc-{kind}-", dir=os.environ.get("TMPDIR", "/tmp"))
                with os.fdopen(fd, "wb") as f:
                    f.write(base64.b64decode(data_b64))
                os.chmod(path, 0o600)
                return path

            if u.get("client-certificate-data") and u.get("client-key-data"):
                self.ctx.load_cert_chain(_tmp("crt", u["client-certificate-data"]),
                                         _tmp("key", u["client-key-data"]))
            elif u.get("client-certificate"):
                self.ctx.load_cert_chain(u["client-certificate"], u["client-key"])

    def _req(self, method, path, body=None, ctype="application/json"):
        url = self.host + path
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        if self._token:
            req.add_header("Authorization", "Bearer " + self._token)
        if data:
            req.add_header("Content-Type", ctype)
        try:
            with urllib.request.urlopen(req, context=self.ctx, timeout=30) as r:
                return r.status, json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            try:
                payload = json.loads(e.read() or b"{}")
            except Exception:
                payload = {}
            return e.code, payload

    def get(self, path):
        code, obj = self._req("GET", path)
        if code != 200:
            raise RuntimeError(f"GET {path} -> {code} {obj.get('message','')}")
        return obj

    def try_get(self, path):
        code, obj = self._req("GET", path)
        return (obj if code == 200 else None)

    def evict(self, ns, name, uid):
        body = {
            "apiVersion": "policy/v1",
            "kind": "Eviction",
            "metadata": {"name": name, "namespace": ns, "uid": uid},
        }
        return self._req("POST", f"/api/v1/namespaces/{ns}/pods/{name}/eviction", body)

# ---------------------------------------------------------------- quantities

_SUF = {"n": 1e-9, "u": 1e-6, "m": 1e-3, "": 1.0, "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15,
        "Ki": 1024, "Mi": 1024 ** 2, "Gi": 1024 ** 3, "Ti": 1024 ** 4, "Pi": 1024 ** 5}

def q(v):
    if not v:
        return 0.0
    v = str(v)
    for suf in ("Ki", "Mi", "Gi", "Ti", "Pi", "n", "u", "m", "k", "M", "G", "T", "P"):
        if v.endswith(suf):
            return float(v[: -len(suf)]) * _SUF[suf]
    return float(v)

def pod_requests(p):
    """Return (cpu, mem) requests; init containers are folded in via max, as the scheduler does."""
    cpu = mem = 0.0
    ic_cpu = ic_mem = 0.0
    for c in p["spec"]["containers"]:
        r = (c.get("resources") or {}).get("requests") or {}
        cpu += q(r.get("cpu"))
        mem += q(r.get("memory"))
    for c in p["spec"].get("initContainers") or []:
        r = (c.get("resources") or {}).get("requests") or {}
        ic_cpu = max(ic_cpu, q(r.get("cpu")))
        ic_mem = max(ic_mem, q(r.get("memory")))
    return max(cpu, ic_cpu), max(mem, ic_mem)

# ---------------------------------------------------------------- affinity

def node_selector_ok(pod, node):
    sel = pod["spec"].get("nodeSelector") or {}
    labels = node["metadata"]["labels"]
    return all(labels.get(k) == v for k, v in sel.items())

def _match_expr(expr, value):
    op, key = expr.get("operator"), expr.get("key")
    vals = expr.get("values") or []
    if op == "In":
        return value in vals
    if op == "NotIn":
        return value not in vals
    if op == "Exists":
        return value is not None
    if op == "DoesNotExist":
        return value is None
    if op in ("Gt", "Lt") and value is not None:
        try:
            return (int(value) > int(vals[0])) if op == "Gt" else (int(value) < int(vals[0]))
        except Exception:
            return False
    return True

def required_node_affinity_ok(pod, node):
    terms = (((pod["spec"].get("affinity") or {}).get("nodeAffinity") or {})
             .get("requiredDuringSchedulingIgnoredDuringExecution") or {}).get("nodeSelectorTerms")
    if not terms:
        return True
    labels = node["metadata"]["labels"]
    for t in terms:
        ok = True
        for e in t.get("matchExpressions") or []:
            if e.get("key") == "metadata.name":
                continue
            if not _match_expr(e, labels.get(e.get("key"))):
                ok = False
                break
        if ok:
            return True
    return False

# ---------------------------------------------------------------- model

def now_ts():
    return datetime.now(timezone.utc).timestamp()

def parse_ts(s):
    if not s:
        return 0.0
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()

class Node:
    def __init__(self, raw, metrics):
        st = raw["status"]
        self.name = raw["metadata"]["name"]
        self.labels = raw["metadata"]["labels"]
        self.raw = raw
        self.alloc_cpu = q(st["allocatable"]["cpu"])
        self.alloc_mem = q(st["allocatable"]["memory"])
        # Physical capacity: kubelet reserve and eviction thresholds live in the
        # capacity-allocatable gap, so OOM risk is about distance from the physical limit --
        # which is why source/hotspot checks use the physical basis.
        self.cap_mem = q((st.get("capacity") or {}).get("memory", 0)) or self.alloc_mem
        self.alloc_pods = int(st["allocatable"]["pods"])
        self.taints = [t.get("key") for t in (raw["spec"].get("taints") or [])]
        self.zone = (raw["metadata"].get("labels") or {}).get("topology.kubernetes.io/zone")
        # pool = pool label (default: group) + CPU class; clusters use different label keys,
        # so point POOL_LABEL at the right one
        self.pool = f"{self.labels.get(CFG['pool_label'], '?')}/{round(self.alloc_cpu)}C"
        self.ready = any(c["type"] == "Ready" and c["status"] == "True" for c in st.get("conditions", []))
        # A node someone else cordoned (node replacement / maintenance) must never be a target:
        # the replacement would have nowhere to go, settle would time out and the round aborts.
        self.unschedulable = bool(raw["spec"].get("unschedulable"))
        self.pressure = any(c["type"] in ("MemoryPressure", "DiskPressure", "PIDPressure") and c["status"] == "True"
                            for c in st.get("conditions", []))
        m = metrics.get(self.name) or {}
        self.mem_used = q((m.get("usage") or {}).get("memory"))
        self.cpu_used = q((m.get("usage") or {}).get("cpu"))
        # real usage (instantaneous; EMA is injected later, see update_ema)
        self.act_cpu_pct = self.cpu_used / self.alloc_cpu * 100 if self.alloc_cpu else 0
        self.act_mem_pct = self.mem_used / self.alloc_mem * 100 if self.alloc_mem else 0
        self.act_samples = 1
        self.pods = []
        self.cpu_req = self.mem_req = 0.0

    def add(self, pod):
        c, m = pod_requests(pod)
        self.cpu_req += c
        self.mem_req += m
        self.pods.append(pod)

    def finalize(self):
        self.cpu_pct = self.cpu_req / self.alloc_cpu * 100 if self.alloc_cpu else 0
        self.mem_pct = self.mem_req / self.alloc_mem * 100 if self.alloc_mem else 0
        self.pod_pct = len(self.pods) / self.alloc_pods * 100 if self.alloc_pods else 0
        self.memuse_pct = self.act_mem_pct
        self.score = max(self.cpu_pct, self.mem_pct, self.pod_pct)

    def driver(self):
        """The tightest dimension of this node. Pod slots come first (slot balance is the
        premise), then real CPU / real memory, then requests (optional). Returns
        (dimension, value) or None."""
        cands = []
        if self.pod_pct >= CFG["pod_high"]:
            cands.append(("pod", self.pod_pct))
        # Pool-pressure driver: when the pool is over POOL_SLOT_PRESSURE, nodes above
        # POOL_PRESSURE_SRC_MIN also count as sources -- otherwise a pool silently climbs to 91%
            # while no single node ever reaches 95%.
        if CFG["pool_slot_pressure"] > 0:
            pu = POOL_USED.get(self.pool)
            others = [v for k, v in POOL_USED.items() if k != self.pool]
            # Only the fullest pool pushes outward; two pools above the line would otherwise push
            # pods at each other.
            if (pu is not None and pu > CFG["pool_slot_pressure"]
                    and pu > max(others, default=0)
                    and self.pod_pct >= CFG["pool_pressure_src_min"]):
                cands.append(("pool_slot", self.pod_pct))
        if CFG["balance_dim"] != "requests":
            if self.act_cpu_pct >= CFG["cpu_act_high"]:
                cands.append(("act_cpu", self.act_cpu_pct))
            if self.hot_mem_pct >= CFG["mem_act_high"]:
                cands.append(("act_mem", self.hot_mem_pct))
        if CFG["balance_dim"] in ("requests", "both"):
            if self.cpu_pct >= CFG["cpu_high"]:
                cands.append(("req_cpu", self.cpu_pct))
            if self.mem_pct >= CFG["mem_high"]:
                cands.append(("req_mem", self.mem_pct))
        # Capacity-pressure fallback: pending pods queued and this node's alloc usage maxed out
        # -> treat it as hot, or it would never be picked.
        if CFG["pending_pressure"] and PENDING["n"] > 0:
            alloc_worst = max(self.cpu_pct, self.mem_pct, self.pod_pct)
            if alloc_worst >= CFG["alloc_pressure_high"]:
                cands.append(("pressure", alloc_worst))
        if not cands:
            return None
        # priority: node slot overload > pool pressure > real CPU/memory > requests
        rank = {"pod": 0, "pool_slot": 1}
        cands.sort(key=lambda x: (rank.get(x[0], 2), -x[1]))
        return cands[0]

    @property
    def mem_guard_pct(self):
        """Denominator for the target memory guard: physical = usage/capacity, alloc = usage/allocatable."""
        return self.act_mem_phys_pct if CFG["target_memuse_basis"] == "physical" else self.act_mem_pct

    @property
    def pool_name(self):
        return self.pool

    @property
    def act_mem_phys_pct(self):
        """The same usage expressed against physical capacity (capacity-allocatable is a fixed reserve)."""
        return self.act_mem_pct * (self.alloc_mem / self.cap_mem) if self.cap_mem else 0

    @property
    def hot_mem_pct(self):
        """Memory basis for source/hotspot checks: physical % when MEM_BASIS=physical, else alloc %.
        Keep this separate from the target guard, which follows the scheduler."""
        return self.act_mem_phys_pct if CFG["mem_basis"] == "physical" else self.act_mem_pct

    def headroom_ok(self):
        # Taints are not checked here: whether a pod may land on a node is decided per pod by
        # tolerations_ok().
        # Filtering on them here would hide whole tainted pools from the balancer for good.
        return (self.ready and not self.pressure and not self.unschedulable
                and self.pod_pct <= CFG["target_pod_max"]
                and self.cpu_pct <= CFG["target_cpu_max"]
                and self.mem_pct <= CFG["target_mem_max"]
                and self.mem_guard_pct <= CFG["target_memuse_max"])

    def fits(self, cpu, mem, src_after_score=None, single=False, act=None, src_act_after=None,
             apply_act_improve=False, zone=None, margin=None):
        """Can this node take the pod? Scheduler-equivalent checks (slots/CPU/memory/taints), then
        does the node stay healthy after the move.

        apply_act_improve: only require real usage to improve when the victim was chosen by
        real usage; slot-driven moves only apply the ceilings, otherwise a cluster with high
        memory everywhere would veto every move.
        zone: when set, the target must share the source zone -- StatefulSet PVs are
        zone-scoped and a cross-zone move stays Pending."""
        if not self.ready or self.pressure:
            return False
        if zone is not None and self.zone != zone:
            return False
        if len(self.pods) + 1 > self.alloc_pods:
            return False
        if self.cpu_req + cpu > self.alloc_cpu or self.mem_req + mem > self.alloc_mem:
            return False
        if self.mem_guard_pct > CFG["target_memuse_max"]:
            return False
        after_cpu = (self.cpu_req + cpu) / self.alloc_cpu * 100
        after_mem = (self.mem_req + mem) / self.alloc_mem * 100
        after_pod = (len(self.pods) + 1) / self.alloc_pods * 100
        if after_cpu > CFG["target_after_cpu_max"]:
            return False
        if after_mem > CFG["target_after_mem_max"]:
            return False
        if after_pod > CFG["target_pod_max"]:
            return False
        if src_after_score is not None:
            margin = CFG["single_improve_margin"] if single else CFG["improve_margin"]
            if max(after_cpu, after_mem, after_pod) > src_after_score - margin:
                return False
        # Real usage: after the move the target must stay under its ceilings and beat the source
        if act and CFG["balance_dim"] != "requests":
            a_cpu, a_mem = act
            act_after_cpu = self.act_cpu_pct + (a_cpu / self.alloc_cpu * 100 if self.alloc_cpu else 0)
            if CFG["target_memuse_basis"] == "physical" and self.cap_mem:
                act_after_mem = self.act_mem_phys_pct + a_mem / self.cap_mem * 100
            else:
                act_after_mem = self.act_mem_pct + (a_mem / self.alloc_mem * 100 if self.alloc_mem else 0)
            if act_after_cpu > CFG["act_after_cpu_max"]:
                return False
            if act_after_mem > CFG["act_after_mem_max"]:
                return False
            # Physical hard cap (capacity denominator, aligned with OOM risk) -- closer to real headroom
            if self.cap_mem:
                phys_after = self.act_mem_phys_pct + a_mem / self.cap_mem * 100
                if phys_after > CFG["phys_after_mem_max"]:
                    return False
            if src_act_after is not None and apply_act_improve:
                m = margin if margin is not None else (CFG["single_improve_margin"] if single else CFG["act_improve_margin"])
                if max(act_after_cpu, act_after_mem) > src_act_after - m:
                    return False
        return True

# ---------------------------------------------------------------- pod filter

def deployment_name(pod):
    for o in pod["metadata"].get("ownerReferences") or []:
        if o["kind"] == "ReplicaSet":
            return o["name"].rsplit("-", 1)[0], o["uid"]
    return None, None

def workload_key(pod):
    """Return the workload name: a Deployment (via its ReplicaSet) or a StatefulSet.
    Derived from ownerReferences only, no extra API calls."""
    owners = pod["metadata"].get("ownerReferences") or []
    if not owners:
        return None
    o = owners[0]
    ns = pod["metadata"]["namespace"]
    if o["kind"] == "ReplicaSet":
        return f"{ns}/{o['name'].rsplit('-', 1)[0]}"
    return f"{ns}/{o['name']}"


def _glob_any(key, patterns):
    return any(fnmatch.fnmatchcase(key, pat) for pat in patterns)


def skip_reason(pod, nodes_by_name, state):
    ns = pod["metadata"]["namespace"]
    meta = pod["metadata"]
    spec = pod["spec"]
    if not re.match(CFG["ns_include"], ns) or ns in CFG["ns_exclude"]:
        return "ns"
    # workload lists (ConfigMap driven, deny wins over allow)
    wkey = workload_key(pod)
    if wkey and CFG["deny_workloads"] and _glob_any(wkey, CFG["deny_workloads"]):
        return "workload-denied"
    if CFG["allow_workloads"] and not (wkey and _glob_any(wkey, CFG["allow_workloads"])):
        return "not-in-allowlist"
    owners = meta.get("ownerReferences") or []
    if not owners:
        return "owner:bare"
    kind = owners[0]["kind"]
    # StatefulSets are untouched by default; namespaces matching ALLOW_STS_NS are opened
    # up (single-replica data stores). They always carry PVCs, so the storage filter is bypassed
    # for them and the zone gate in fits() takes over.
    sts_allowed = (kind == "StatefulSet"
                   and (CFG["allow_statefulset"]
                        or (CFG["allow_sts_ns"] and re.match(CFG["allow_sts_ns"], ns))))
    if kind != "ReplicaSet" and not sts_allowed:
        return f"owner:{kind}"
    if meta.get("deletionTimestamp"):
        return "terminating"
    if spec.get("nodeName") is None:
        return "unscheduled"
    if pod["status"].get("phase") != "Running":
        return "not-running"
    if not CFG["allow_hostnetwork"]:
        if spec.get("hostNetwork") or spec.get("hostPID"):
            return "host-network/pid"
        if any(p.get("hostPort") for c in spec["containers"] for p in (c.get("ports") or [])):
            return "host-port"
    if not CFG["allow_storage"] and not sts_allowed:
        if any(v.get("persistentVolumeClaim") or v.get("hostPath")
               for v in spec.get("volumes") or []):
            return "storage"
    req = (spec.get("affinity") or {}).get("podAntiAffinity") or {}
    if not CFG["allow_required_antiaffinity"] and req.get("requiredDuringSchedulingIgnoredDuringExecution"):
        return "required-antiaffinity"
    for cs in pod["status"].get("containerStatuses") or []:
        if not cs.get("ready"):
            if CFG["allow_not_ready"]:
                continue
            return "not-ready"
        if (cs.get("state") or {}).get("waiting"):
            return "waiting:" + ((cs["state"]["waiting"].get("reason")) or "?")
        if cs.get("restartCount", 0) > CFG["max_restarts"]:
            return "restarts"
    if now_ts() - parse_ts(meta.get("creationTimestamp")) < CFG["min_age_min"] * 60:
        return "too-young"
    name, _ = deployment_name(pod)
    fp = f"{ns}/{name}" if name else wkey
    cu = state["cooldowns"].get(fp, 0)
    if cu > now_ts():
        return "cooldown"
    return None

# ---------------------------------------------------------------- planner

def load_state(k8s):
    ns, name = CFG["state_cm"]
    cm = k8s.try_get(f"/api/v1/namespaces/{ns}/configmaps/{name}")
    st = {"cooldowns": {}, "moves": [], "last_run": None}
    if cm:
        try:
            st.update(json.loads(cm["data"]["state"]))
        except Exception:
            pass
    return st, cm

def save_state(k8s, state, cm, summary):
    ns, name = CFG["state_cm"]
    cutoff = now_ts() - 86400
    state["moves"] = [m for m in state["moves"] if m > cutoff]
    state["cooldowns"] = {k: v for k, v in state["cooldowns"].items() if v > now_ts() - 86400}
    state["last_run"] = summary
    body = {"apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": name, "namespace": ns},
            "data": {"state": json.dumps(state, ensure_ascii=False)}}
    if cm:
        code, _ = k8s._req("PUT", f"/api/v1/namespaces/{ns}/configmaps/{name}", body)
    else:
        code, _ = k8s._req("POST", f"/api/v1/namespaces/{ns}/configmaps", body)
    return code

def set_mode(k8s, mode):
    """Write MODE back to the parameter ConfigMap (auto-downgrade). Returns the HTTP code or None."""
    ns, name = CFG["cfg_cm"]
    cm = k8s.try_get(f"/api/v1/namespaces/{ns}/configmaps/{name}")
    if not cm:
        return None
    data = dict(cm.get("data") or {})
    data["MODE"] = mode
    body = {"apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": name, "namespace": ns}, "data": data}
    code, _ = k8s._req("PUT", f"/api/v1/namespaces/{ns}/configmaps/{name}", body)
    return code

def slim_pod(p):
    """Keep only the fields decisions need: a raw pod object is ~10KB, and 1750 of them blow up
    a small container."""
    md, sp, stt = p["metadata"], p["spec"], p["status"]
    return {
        "metadata": {
            "name": md["name"], "namespace": md["namespace"], "uid": md["uid"],
            "creationTimestamp": md.get("creationTimestamp"),
            "deletionTimestamp": md.get("deletionTimestamp"),
            "ownerReferences": [{k: o.get(k) for k in ("kind", "name", "uid")}
                                for o in (md.get("ownerReferences") or [])],
        },
        "spec": {
            "nodeName": sp.get("nodeName"),
            "hostNetwork": sp.get("hostNetwork"), "hostPID": sp.get("hostPID"),
            "nodeSelector": sp.get("nodeSelector"),
            "tolerations": sp.get("tolerations"),
            "affinity": sp.get("affinity"),
            "containers": [{"name": c.get("name"), "resources": c.get("resources"),
                            "ports": [{"hostPort": x.get("hostPort")} for x in (c.get("ports") or [])]}
                           for c in (sp.get("containers") or [])],
            "initContainers": [{"resources": c.get("resources")} for c in (sp.get("initContainers") or [])],
            "volumes": [{"persistentVolumeClaim": v.get("persistentVolumeClaim"),
                         "hostPath": v.get("hostPath")} for v in (sp.get("volumes") or [])],
        },
        "status": {
            "phase": stt.get("phase"),
            "containerStatuses": [{"ready": c.get("ready"), "restartCount": c.get("restartCount", 0),
                                   "state": c.get("state")}
                                  for c in (stt.get("containerStatuses") or [])],
        },
    }

def list_pods_slim(k8s, page_size=500, field_selector=None):
    """Fetch page by page and slim each page, instead of parsing a whole multi-MB PodList at once."""
    out, cont = [], None
    while True:
        path = f"/api/v1/pods?limit={page_size}"
        if field_selector:
            path += f"&fieldSelector={urllib.parse.quote(field_selector)}"
        if cont:
            path += f"&continue={cont}"
        r = k8s.get(path)
        for p in r.get("items") or []:
            out.append(slim_pod(p))
        cont = (r.get("metadata") or {}).get("continue")
        if not cont:
            break
    return out

def check_landing(k8s, name):
    """Re-measure the landing node: the scheduler picked it, so it has to be checked afterwards."""
    raw = k8s.try_get(f"/api/v1/nodes/{name}")
    if not raw:
        return None
    met = {m["metadata"]["name"]: m for m in
           (k8s.try_get("/apis/metrics.k8s.io/v1beta1/nodes") or {}).get("items", [])}
    n = Node(raw, met)
    for p in list_pods_slim(k8s, field_selector=f"spec.nodeName={name}"):
        if p["spec"].get("nodeName") == name and p["status"].get("phase") == "Running":
            n.add(p)
    n.finalize()
    return {"pods": len(n.pods), "alloc_pods": n.alloc_pods,
            "pod_pct": round(n.pod_pct, 1), "cpu_pct": round(n.cpu_pct, 1),
            "mem_pct": round(n.mem_pct, 1), "memuse_pct": round(n.memuse_pct, 1)}

def landing_guard_violation(g):
    """Did the landing node break one of our own ceilings (slots / real memory / real CPU)?"""
    if not g:
        return None
    if g["pod_pct"] > CFG["target_pod_max"]:
        return "pod_slots"
    if g["memuse_pct"] > CFG["target_memuse_max"]:
        return "mem_actual"
    if g["cpu_pct"] > CFG["target_after_cpu_max"]:
        return "cpu"
    return None

def fetch_pod_act(k8s):
    """Real usage per pod (summed over containers) -> {(ns, name): (cpu cores, bytes)}"""
    out = {}
    for m in (k8s.try_get("/apis/metrics.k8s.io/v1beta1/pods") or {}).get("items", []):
        cu = sum(q((c.get("usage") or {}).get("cpu")) for c in (m.get("containers") or []))
        mu = sum(q((c.get("usage") or {}).get("memory")) for c in (m.get("containers") or []))
        out[(m["metadata"]["namespace"], m["metadata"]["name"])] = (cu, mu)
    return out

def update_ema(state, nodes, pod_act):
    """EMA smoothing: node level (damps instant spikes that cause flapping) and workload level
    (used by cooldowns and notifications)."""
    ema = state.setdefault("ema", {"nodes": {}, "workloads": {}})
    a = CFG["act_alpha"]

    def blend(old, cur):
        if not old:
            return {"cpu": cur[0], "mem": cur[1], "n": 1}
        return {"cpu": old["cpu"] * (1 - a) + cur[0] * a,
                "mem": old["mem"] * (1 - a) + cur[1] * a,
                "n": old.get("n", 1) + 1}

    for name, n in nodes.items():
        if not n.alloc_cpu:
            continue
        cur = (n.cpu_used / n.alloc_cpu * 100, n.mem_used / n.alloc_mem * 100)
        ema["nodes"][name] = blend(ema["nodes"].get(name), cur)
    wl = {}
    for n in nodes.values():
        for p in n.pods:
            key = workload_key(p)
            if not key:
                continue
            cu, mu = pod_act.get((p["metadata"]["namespace"], p["metadata"]["name"]), (0.0, 0.0))
            agg = wl.setdefault(key, [0.0, 0.0])
            agg[0] += cu
            agg[1] += mu
    for key, (cu, mu) in wl.items():
        ema["workloads"][key] = blend(ema["workloads"].get(key), (cu, mu))
    return ema

def set_cordon(k8s, name, cordon):
    """Cordon or uncordon a node. Cordoned nodes get a label whose value is a timestamp, so
    the next round can recognise them and can tell how long they have been locked even if
    this process was killed."""
    if cordon:
        body = {"metadata": {"labels": {CFG["cordon_label"]: str(int(time.time()))}},
                "spec": {"unschedulable": True}}
    else:
        body = {"metadata": {"labels": {CFG["cordon_label"]: None}},
                "spec": {"unschedulable": False}}
    code, _ = k8s._req("PATCH", f"/api/v1/nodes/{name}", body,
                       ctype="application/merge-patch+json")
    return code

def heal_cordons(k8s):
    """Heal cordons left behind by an interrupted round. Only nodes carrying our own label are
    touched; anyone else's cordon is left alone. The label value is the cordon timestamp:
    anything older than CORDON_TTL_MIN is released unconditionally, and younger ones are
    released too -- a second round starting concurrently would break the first one's window."""
    healed = []
    try:
        for n in k8s.get("/api/v1/nodes")["items"]:
            labels_ = n["metadata"].get("labels") or {}
            raw = labels_.get(CFG["cordon_label"])
            if raw is None:
                raw = labels_.get(CFG["cordon_label_legacy"]) if CFG["cordon_label_legacy"] else None
            if raw is None:
                continue
            try:
                age_min = (time.time() - int(raw)) / 60
            except (TypeError, ValueError):
                age_min = -1        # legacy ("true") or malformed value: treat as expired
            if set_cordon(k8s, n["metadata"]["name"], False) in (200, 204):
                healed.append({"node": n["metadata"]["name"], "age_min": round(age_min, 1)})
    except Exception as e:
        report_line("cordon-heal-failed", err=f"{type(e).__name__}: {e}")
    if healed:
        report_line("cordon-healed", nodes=[h["node"] for h in healed], detail=healed,
                    why="cordons left by an interrupted round, scheduled again")
    return [h["node"] for h in healed]

def cordon_bad_targets(k8s, src, target, scope, pod=None, sts=False):
    """Landing guidance: temporarily lock the nodes this pod could land on but that are not
    the computed target, so kube-scheduler picks the target. The window is one evict+settle.

    Which nodes get locked follows the pod's own placement rules (nodeSelector, tolerations,
    StatefulSet zone) rather than a hard-coded pool name: narrowing this to one pool sent
    zone-bound pods straight back to their source node, bouncing repeatedly."""
    done = []
    for n in scope.values():
        if n.name == target.name:
            continue
        if pod is not None:
            if not node_selector_ok(pod, n.raw):
                continue
            if not tolerations_ok(pod, n):
                continue
            if sts and n.zone != src.zone:
                continue
        elif not _glob_any(n.pool_name, CFG["cordon_pools"]):
            continue
        # With pod context: lock every node it could land on, except the target.
        # Without pod context (legacy callers): fall back to locking nodes the ceilings exclude anyway.
        bad = True if pod is not None else (n.name == src.name
               or n.pod_pct > CFG["target_pod_max"]
               or n.act_mem_pct > CFG["target_memuse_max"]
               or n.cpu_pct > CFG["target_cpu_max"])
        if bad and set_cordon(k8s, n.name, True) in (200, 201, 204):
            done.append(n.name)
    return done

def uncordon_all(k8s, names):
    for name in names:
        set_cordon(k8s, name, False)

def build_world(k8s, state=None):
    nodes_raw = k8s.get("/api/v1/nodes")["items"]
    metrics = {m["metadata"]["name"]: m for m in
               (k8s.try_get("/apis/metrics.k8s.io/v1beta1/nodes") or {}).get("items", [])}
    nodes = {n["metadata"]["name"]: Node(n, metrics) for n in nodes_raw}
    pod_act = fetch_pod_act(k8s)
    all_pods = list_pods_slim(k8s)
    # Capacity-pressure gate: count unschedulable Pending pods (no nodeName, older than N
    # minutes, so transient pods are ignored)
    _now = now_ts()
    PENDING["n"] = sum(
        1 for p in all_pods
        if p["status"].get("phase") == "Pending" and not p["spec"].get("nodeName")
        and CFG["pending_min_age_min"] * 60 < _now - parse_ts(p["metadata"].get("creationTimestamp")) < 86400)
    # Only the last 24h: a pod stuck for weeks is a configuration leftover, not a capacity signal.
    pods = [p for p in all_pods
            if p["spec"].get("nodeName") in nodes and p["status"].get("phase") == "Running"]
    for p in pods:
        key = (p["metadata"]["namespace"], p["metadata"]["name"])
        p["_act"] = pod_act.get(key, (0.0, 0.0))
        p["_act_known"] = key in pod_act
    rs_replicas = {}
    for p in pods:
        _, rsuid = deployment_name(p)
        if rsuid:
            rs_replicas[rsuid] = rs_replicas.get(rsuid, 0) + 1
    for p in pods:
        nodes[p["spec"]["nodeName"]].add(p)
    if state is not None:
        ema = update_ema(state, nodes, pod_act)
        for name, n in nodes.items():
            e = (ema.get("nodes") or {}).get(name)
            if e and e.get("n", 0) >= CFG["act_min_samples"]:
                n.act_cpu_pct, n.act_mem_pct = e["cpu"], e["mem"]
                n.act_samples = e["n"]
    for n in nodes.values():
        n.finalize()
    return nodes, rs_replicas

def in_scope(nodes):
    return {k: n for k, n in nodes.items()
            if n.pool_name in CFG["pools"] and n.ready}

def pick_targets(src, all_nodes):
    """Candidate targets: no same-pool restriction. Any node with headroom gets a chance;
    whether it can really take the pod is decided pod by pod in Node.fits()."""
    out = []
    for n in all_nodes.values():
        if n.name == src.name or n.name in ROUND_SOURCES or not n.headroom_ok():
            continue
        if not CFG["allow_cross_pool"] and n.pool != src.pool:
            continue
        out.append(n)
    return sorted(out, key=lambda n: (0 if n.pool == src.pool else 1, n.score))

def rank_victims(src, driver, rs_replicas, nodes_by_name, state):
    """Order victims by the source's tightest dimension:
       act_cpu  -> pick the largest real CPU consumer (moves the heat)
       act_mem  -> pick the largest real memory consumer
       req_cpu / req_mem -> pick the largest requests
       pod      -> any pod frees a slot; pick the largest real consumer
       multi-replica workloads always come first (no interruption)"""
    cands = []
    for p in src.pods:
        reason = skip_reason(p, nodes_by_name, state)
        if reason:
            continue
        # No metrics sample -> real memory/CPU unknown, so leave it alone this round
        if CFG["act_missing_policy"] == "skip" and not p.get("_act_known", False):
            NO_METRICS["n"] += 1
            continue
        cpu, mem = pod_requests(p)
        if cpu > CFG["max_pod_cpu"] or mem > CFG["max_pod_mem_gi"] * 2 ** 30:
            continue
        _, rsuid = deployment_name(p)
        a_cpu, a_mem = p.get("_act") or (0.0, 0.0)
        cands.append({"pod": p, "cpu": cpu, "mem": mem,
                      "act_cpu": a_cpu, "act_mem": a_mem,
                      "multi": rs_replicas.get(rsuid, 1) > 1,
                      "sts": (p["metadata"].get("ownerReferences") or [{}])[0].get("kind") == "StatefulSet",
                      "age": now_ts() - parse_ts(p["metadata"]["creationTimestamp"])})
    if driver == "act_cpu":
        relief = lambda c: (-c["act_cpu"], -c["act_mem"])
    elif driver == "act_mem":
        relief = lambda c: (-c["act_mem"], -c["act_cpu"])
    elif driver == "req_cpu":
        relief = lambda c: (-c["cpu"], -c["mem"])
    elif driver == "req_mem":
        relief = lambda c: (-c["mem"], -c["cpu"])
    elif driver == "pressure":
        # Capacity pressure: move the largest request holder first (frees the most schedulable room)
        relief = lambda c: (-c["mem"], -c["cpu"])
    else:
        # Slots full: any pod frees a slot, so prefer the largest real consumer --
        # one move relieves both slot pressure and real heat, instead of just picking the smallest pod
        relief = lambda c: (-c["act_cpu"], -c["act_mem"])
    cands.sort(key=lambda c: (0 if (CFG["prefer_multi_replica"] and c["multi"]) else 1, relief(c), -c["age"]))
    return cands

def tolerations_ok(pod, node):
    """Can the pod tolerate the target node's taints? Same-shape nodes are usually untainted;
    across pools this is a hard gate."""
    taints = node.raw["spec"].get("taints") or []
    if not taints:
        return True
    tols = pod["spec"].get("tolerations") or []
    for t in taints:
        if t.get("effect") not in (None, "NoSchedule", "NoExecute"):
            continue
        ok = False
        for tol in tols:
            if tol.get("operator") == "Exists" and tol.get("key") in (None, "", t.get("key")):
                ok = True
            elif tol.get("key") == t.get("key") and tol.get("value") == t.get("value"):
                ok = True
            if ok:
                break
        if not ok:
            return False
    return True

def choose_move(src, driver, targets, rs_replicas, nodes_by_name, state, quota, tried, plan_count):
    for c in rank_victims(src, driver, rs_replicas, nodes_by_name, state):
        if c["pod"]["metadata"]["uid"] in tried:
            continue
        if not c["multi"] and quota["single"] <= 0:
            continue
        # Single-replica StatefulSets: their own quota (0 = disabled), at most max_sts_per_cycle per round
        if c.get("sts") and quota.get("sts", 0) <= 0:
            continue
        # (1) The source must genuinely improve, measured on the dimension that drove the
        # move. Thresholds are absolute resource amounts, never percentage points: the same
        # pod is 12.5 points on a 4-core node but 3.1 points on a 16-core one, so a point
        # floor silently freezes the large machines. Slot dimensions free exactly one slot
        # per eviction and take no floor at all; MAX_PER_CYCLE / MAX_PER_NODE / DAILY_BUDGET
        # already bound how much churn a round may cause.
        relief_ok = True
        if driver == "act_cpu":
            relief_ok = c["act_cpu"] >= CFG["relief_cores"]
        elif driver == "act_mem":
            relief_ok = c["act_mem"] / 2 ** 30 >= CFG["relief_gi"]
        elif driver == "req_cpu":
            relief_ok = c["cpu"] >= CFG["relief_cores"]
        elif driver == "req_mem":
            relief_ok = c["mem"] / 2 ** 30 >= CFG["relief_gi"]
        elif driver == "pressure":
            relief_ok = (c["cpu"] >= CFG["relief_cores"]
                         or c["mem"] / 2 ** 30 >= CFG["relief_gi"])
        if not relief_ok:
            continue
        # The requests view (what the scheduler sees) must not get worse either
        src_after = max(
            (src.cpu_req - c["cpu"]) / src.alloc_cpu * 100,
            (src.mem_req - c["mem"]) / src.alloc_mem * 100,
            (len(src.pods) - 1) / src.alloc_pods * 100)
        # Real usage: what the source looks like after the pod leaves
        src_act_after = max(
            src.act_cpu_pct - (c["act_cpu"] / src.alloc_cpu * 100 if src.alloc_cpu else 0),
            src.act_mem_pct - (c["act_mem"] / src.alloc_mem * 100 if src.alloc_mem else 0))
        # Same pool first (least disruption to request layout), then spread by planned moves
        ordered = sorted(targets, key=lambda n: (0 if n.pool == src.pool else 1,
                                                 plan_count.get(n.name, 0), n.score))
        for t in ordered:
            pod = c["pod"]
            if not node_selector_ok(pod, t.raw):
                continue
            if not required_node_affinity_ok(pod, t.raw):
                continue
            if not tolerations_ok(pod, t):
                continue
            # StatefulSets are small single-replica objects; comparing request percentages across node
            # shapes is meaningless: 46 slots vs 211 slots is not the same scale.
            # Pool pressure with an emptier target pool -> skip the node-percentage comparison:
            # removing one pod from a small node can drop 2 points, while adding one to a large node
            # raises it 0.5; comparing percentages vetoes moves that clearly help the pool.
            # Safety still comes from the ceilings: slots, CPU, memory, physical memory.
            skip_pct_test = c.get("sts") or (
                driver == "pool_slot" and POOL_USED.get(t.pool_name, 100) < POOL_USED.get(src.pool_name, 0))
            if not t.fits(c["cpu"], c["mem"],
                          src_after_score=None if skip_pct_test else src_after,
                          single=(not c["multi"] and not c.get("sts")),
                          act=(c["act_cpu"], c["act_mem"]), src_act_after=src_act_after,
                          apply_act_improve=driver in ("act_cpu", "act_mem"),
                          zone=src.zone if c.get("sts") else None,
                          margin=CFG["sts_improve_margin"] if c.get("sts") else None):
                continue
            return c, t
    return None, None

def explain(src, rs_replicas, nodes_by_name, state):
    """Count why each pod on the source was skipped -- during a dry-run week this shows whether
    the allow-lists are wired correctly."""
    from collections import Counter
    cnt = Counter()
    for p in src.pods:
        cnt[skip_reason(p, nodes_by_name, state) or "candidate"] += 1
    return dict(cnt.most_common(8))


CRASH_REASONS = ("ImagePullBackOff", "ErrImagePull", "CrashLoopBackOff")

def workload_already_bad(scope, pod, is_sts=False):
    """Was this workload already failing **before** the move?

    A Deployment was seen with a replica that had been CrashLoopBackOff thousands of times
    over weeks; moving it produced another crashing pod, and the old logic treated that as
    an incident of its own, aborting the round and downgrading the whole balancer. When the
    workload was already broken, only cool it down instead of aborting."""
    def bad(p):
        for cs in (p.get("status", {}).get("containerStatuses") or []):
            if cs.get("restartCount", 0) >= 3:
                return True
            if (cs.get("state", {}).get("waiting", {}) or {}).get("reason") in CRASH_REASONS:
                return True
        return False
    if is_sts:
        return bad(pod)
    owners = pod["metadata"].get("ownerReferences") or []
    if not owners:
        return False
    rsuid = owners[0]["uid"]
    for n in scope.values():
        for q in n.pods:
            if q["metadata"]["uid"] == pod["metadata"]["uid"]:
                continue
            o = q["metadata"].get("ownerReferences") or []
            if o and o[0].get("uid") == rsuid and bad(q):
                return True
    return False

def wait_settle(k8s, ns, rsuid, old_uid, src_node, timeout, since_ts=None):
    """Wait for the replacement pod; returns (status, node name, pod name).

    since_ts: only accept pods created at or after this moment. Without it, another replica
    of the same ReplicaSet is mistaken for the replacement (this happened in practice)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            pods = k8s.get(f"/api/v1/namespaces/{ns}/pods")["items"]
        except Exception:
            time.sleep(5)
            continue
        for p in pods:
            owners = p["metadata"].get("ownerReferences") or []
            if not owners or owners[0]["uid"] != rsuid or p["metadata"]["uid"] == old_uid:
                continue
            if since_ts is not None and parse_ts(p["metadata"].get("creationTimestamp")) < since_ts - 5:
                continue
            phase = p["status"].get("phase")
            ready = all(cs.get("ready") for cs in p["status"].get("containerStatuses") or []) and \
                bool(p["status"].get("containerStatuses"))
            if phase == "Running" and ready:
                return ("ok", p["spec"].get("nodeName"), p["metadata"]["name"])
            for cs in p["status"].get("containerStatuses") or []:
                w = (cs.get("state") or {}).get("waiting") or {}
                if w.get("reason") in ("ImagePullBackOff", "ErrImagePull", "CrashLoopBackOff"):
                    return (w["reason"], p["spec"].get("nodeName"), p["metadata"]["name"])
        time.sleep(5)
    return ("timeout", None, None)

def wait_settle_sts(k8s, ns, name, old_uid, src_node, timeout):
    """Wait for the StatefulSet pod to be recreated with a new UID and become Running/Ready.
    Landing back on the source node counts as a bounce.

    StatefulSet pod names do not change (app-0), so the Deployment path (a new pod in the
    same ReplicaSet) does not apply."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        p = None
        try:
            p = k8s.get(f"/api/v1/namespaces/{ns}/pods/{name}")
        except Exception:
            time.sleep(5)
            continue
        phase = (p.get("status") or {}).get("phase")
        node = p["spec"].get("nodeName")
        css = (p.get("status") or {}).get("containerStatuses") or []
        ready = bool(css) and all(cs.get("ready") for cs in css)
        for cs in css:
            w = (cs.get("state") or {}).get("waiting") or {}
            if w.get("reason") in ("ImagePullBackOff", "ErrImagePull", "CrashLoopBackOff"):
                return (w["reason"], node, name)
        uid = p["metadata"].get("uid")
        if uid and uid != old_uid and phase == "Running" and ready:
            return ("ok", node, name)
        time.sleep(5)
    return ("timeout", None, None)

# ---------------------------------------------------------------- main

def report_line(kind, **kw):
    print(json.dumps({"t": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                      "kind": kind, **kw}, ensure_ascii=False), flush=True)

# ------------------------------------------------------------- notifications

def http_post_json(url, payload, timeout=10):
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()[:200].decode(errors="replace")

def notify(state, reason, lines, force=False):
    """DingTalk notification. The token comes from an env var injected by a Secret (never from
    the ConfigMap or git). Without a token this only logs notify-skipped and balancing
    continues. Alerts of the same class are rate limited."""
    now = now_ts()
    sent = state.setdefault("notify", {})
    if not CFG["notify_enabled"]:
        report_line("notify-skipped", reason=reason, why="NOTIFY_ENABLED=false")
        return False
    if not force and now - sent.get(reason, 0) < CFG["notify_min_interval_min"] * 60:
        report_line("notify-throttled", reason=reason,
                    next_in_min=round((CFG["notify_min_interval_min"] * 60 - (now - sent.get(reason, 0))) / 60))
        return False
    text = "\n\n".join([f"### {CFG['notify_title']} | {reason}"] + lines)
    token = CFG["dingtalk_token"]
    if not token:
        report_line("notify-skipped", reason=reason,
                    why="DINGTALK_TOKEN is not configured; inject it from a Secret to enable notifications",
                text=text[:800])
        sent[reason] = now
        return False
    url = "https://oapi.dingtalk.com/robot/send?access_token=" + token
    if CFG["dingtalk_secret"]:
        import base64
        import hashlib
        import hmac
        ms = str(int(now * 1000))
        sign = base64.b64encode(hmac.new(CFG["dingtalk_secret"].encode(),
                                         f"{ms}\n{CFG['dingtalk_secret']}".encode(),
                                         hashlib.sha256).digest()).decode()
        url += f"&timestamp={ms}&sign={urllib.parse.quote_plus(sign)}"
    try:
        code, body = http_post_json(url, {"msgtype": "markdown",
                                          "markdown": {"title": CFG["notify_title"], "text": text}})
        sent[reason] = now
        report_line("notified", reason=reason, http=code, resp=body[:160])
        return True
    except Exception as e:
        report_line("notify-failed", reason=reason, err=f"{type(e).__name__}: {e}")
        return False

def main():
    if CFG["mode"] == "off":
        report_line("skipped", why="mode=off")
        return 0
    k8s = K8s()
    state, state_cm = load_state(k8s)
    # Single-instance lock: with a scheduled round and a manual round running together, the
    # later one would heal the earlier one's cordons and both would evict at once. The lock
# lives in our own state ConfigMap and expires on its own if the process dies.
    lock_until = state.get("lock_until", 0) or 0
    if lock_until > now_ts():
        report_line("locked", until=int(lock_until), left_sec=int(lock_until - now_ts()),
                    why="another round is running (or a previous one died holding the lock)")
        return 0
    state["lock_until"] = now_ts() + CFG["max_per_cycle"] * (CFG["settle_timeout"] + 60)
    save_state(k8s, state, state_cm, {"mode": CFG["mode"], "locked": True})

    def _unlock():
        try:
            state["lock_until"] = 0
            save_state(k8s, state, state_cm, {"mode": CFG["mode"], "unlocked": True})
        except Exception:
            pass
    atexit.register(_unlock)
    # When the Job hits activeDeadlineSeconds it gets SIGTERM first (Python exits without
    # running finally blocks), which leaks cordons and leaves nodes unschedulable.
    # Converting it to SystemExit lets the cordon->evict->settle finally block release them.
    def _on_term(_signo, _frame):
        raise SystemExit(143)
    try:
        signal.signal(signal.SIGTERM, _on_term)
        signal.signal(signal.SIGINT, _on_term)
    except Exception:
        pass

    if CFG["cordon_guide"]:
        heal_cordons(k8s)
    nodes, rs_replicas = build_world(k8s, state)
    scope = in_scope(nodes)
    refresh_pool_used(scope)
    ts = now_ts()
    moved_today = [m for m in state["moves"] if m > ts - 86400]

    summary = {"mode": CFG["mode"], "pool": {}, "moves": [], "skipped": []}
    for pool in CFG["pools"]:
        pn = {k: n for k, n in scope.items() if n.pool_name == pool}
        if not pn:
            continue
        vals = [n.cpu_pct for n in pn.values()]
        cap = sum(n.alloc_pods for n in pn.values())
        used = sum(len(n.pods) for n in pn.values())
        summary["pool"][pool] = {"nodes": len(pn), "cpu_min": round(min(vals), 1),
                                 "cpu_max": round(max(vals), 1), "spread": round(max(vals) - min(vals), 1),
                                 "free_slots": cap - used,
                                 "free_pct": round((cap - used) / cap * 100, 1) if cap else 0,
                                 "slot_used_pct": round(used / cap * 100, 1) if cap else 0,
                                 "used_slots": used, "cap_slots": cap}

    quota = {"cycle": CFG["max_per_cycle"], "single": CFG["max_single_replica"],
             "sts": CFG["max_sts_per_cycle"],
             "per_node": {}, "per_ns": {}, "daily": max(0, CFG["daily_budget"] - len(moved_today))}

    report_line("config", **{k: (sorted(v) if isinstance(v, set) else v) for k, v in CFG.items()})
    report_line("start", mode=CFG["mode"], nodes=len(scope), moved_24h=len(moved_today),
                pools=summary["pool"])

    # Sources ordered by their tightest dimension, pod slots first (slot balance is the premise)
    srcs = []
    for n in scope.values():
        d = n.driver()
        if d:
            srcs.append((n, d[0], d[1]))
    srcs.sort(key=lambda x: (0 if (x[1] == "act_mem" and x[2] >= CFG["mem_critical"])
                             else {"pod": 1, "pool_slot": 2}.get(x[1], 3), -x[2]))
    ROUND_SOURCES.clear()
    ROUND_SOURCES.update(n.name for n, _dr, _v in srcs)
    report_line("sources", count=len(srcs), basis=CFG["mem_basis"],
                detail=[{"node": n.name, "driver": dr, "value": round(v, 1),
                         "act_cpu": round(n.act_cpu_pct, 1), "act_mem": round(n.act_mem_pct, 1),
                         "act_mem_phys": round(n.act_mem_phys_pct, 1),
                         "pod_pct": round(n.pod_pct, 1)} for n, dr, v in srcs][:12])

    saturated = set()
    first_action_done = False
    for src, driver, _sev in srcs:
        if src.pool_name in saturated:
            continue
        pool_nodes = {k: n for k, n in scope.items() if n.pool_name == src.pool_name}
        cap = sum(n.alloc_pods for n in pool_nodes.values())
        used = sum(len(n.pods) for n in pool_nodes.values())
        free_pct = (cap - used) / cap * 100 if cap else 0
        if free_pct < CFG["pool_min_free_pct"]:
            # This pool's slots are exhausted; with cross-pool moves allowed the pod can still go elsewhere
            if CFG["allow_cross_pool"]:
                report_line("pool-saturated-local", pool=src.pool_name, free_slots=cap - used,
                            free_pct=round(free_pct, 1), cross_pool=True,
                            why="this pool is out of slots; falling back to cross-pool placement")
            else:
                saturated.add(src.pool_name)
                report_line("pool-saturated", pool=src.pool_name, free_slots=cap - used,
                            free_pct=round(free_pct, 1),
                            why="slots are exhausted: a capacity problem, not a distribution problem; pool skipped")
                summary["skipped"].append({"pool": src.pool_name, "why": "pool-saturated"})
                continue
        if quota["cycle"] <= 0 or quota["daily"] <= 0:
            report_line("budget-exhausted")
            break
        targets = pick_targets(src, scope)
        if not targets:
            report_line("no-target", src=src.name,
                        cpu=round(src.cpu_pct, 1), mem=round(src.mem_pct, 1), pod=round(src.pod_pct, 1),
                        why=f"no node satisfies all of: "
                            f"pod<={CFG['target_pod_max']}%, cpu<={CFG['target_cpu_max']}%, "
                            f"memory<={CFG['target_mem_max']}%, real memory<={CFG['target_memuse_max']}%")
            summary["skipped"].append({"src": src.name, "why": "no-target"})
            continue
        n_moved = 0
        tried = set()
        plan_count = {}
        for _ in range(CFG["max_per_node"]):
            if quota["cycle"] <= 0 or quota["daily"] <= 0:
                break
            c, t = choose_move(src, driver, targets, rs_replicas, nodes, state, quota, tried, plan_count)
            if not c:
                break
            pod = c["pod"]
            ns, name = pod["metadata"]["namespace"], pod["metadata"]["name"]
            # Record whether the workload was already failing, so a failed move can be classified
            c["pre_bad"] = workload_already_bad(scope, pod, bool(c.get("sts")))
            if quota["per_ns"].get(ns, 0) >= CFG["max_per_ns"]:
                tried.add(pod["metadata"]["uid"])
                continue
            fp = f"{ns}/{deployment_name(pod)[0]}" if not c.get("sts") else workload_key(pod)
            rec = {"src": src.name, "pod": f"{ns}/{name}", "target_est": t.name,
                   "workload_kind": "StatefulSet" if c.get("sts") else "Deployment",
                   "driver": driver, "src_act_cpu": round(src.act_cpu_pct, 1),
                   "src_act_mem": round(src.act_mem_pct, 1),
                   "src_act_mem_phys": round(src.act_mem_phys_pct, 1),
                   "pod_act_m": round(c["act_cpu"] * 1000),
                   "pod_act_mi": round(c["act_mem"] / 2 ** 20),
                   "src_pool": src.pool, "target_pool": t.pool,
                   "cross_pool": t.pool != src.pool,
                   "cpu": round(c["cpu"], 2), "mem_mi": round(c["mem"] / 2 ** 20),
                   "multi_replica": c["multi"], "action": CFG["mode"]}

            if CFG["mode"] == "dry-run":
                report_line("plan", **rec)
                # Dry-run must keep the same accounting (source down, target up, slots included), otherwise
            # a target is treated as having unlimited capacity
                src.cpu_req -= c["cpu"]; src.mem_req -= c["mem"]
                src.pods.remove(pod); src.finalize()
                t.cpu_req += c["cpu"]; t.mem_req += c["mem"]; t.pods.append(pod); t.finalize()
                n_moved += 1
                quota["cycle"] -= 1
                quota["per_ns"][ns] = quota["per_ns"].get(ns, 0) + 1
                plan_count[t.name] = plan_count.get(t.name, 0) + 1
                if not c["multi"]:
                    quota["single"] -= 1
                if c.get("sts"):
                    quota["sts"] -= 1
                continue

            if CFG["mode"] == "live" and not first_action_done:
                first_action_done = True
                def _after(n, rem=None, add=None):
                    c_ = n.cpu_req - (rem[0] if rem else 0) + (add[0] if add else 0)
                    m_ = n.mem_req - (rem[1] if rem else 0) + (add[1] if add else 0)
                    k_ = len(n.pods) - (1 if rem else 0) + (1 if add else 0)
                    return {"cpu": round(c_ / n.alloc_cpu * 100, 1),
                            "mem": round(m_ / n.alloc_mem * 100, 1),
                            "pod": round(k_ / n.alloc_pods * 100, 1)}
                report_line("live-first-action",
                            pod=f"{ns}/{name}", src=src.name, target=t.name,
                            src_pool=src.pool, target_pool=t.pool, cross_pool=t.pool != src.pool,
                            src_after=_after(src, rem=(c["cpu"], c["mem"])),
                            target_after=_after(t, add=(c["cpu"], c["mem"])))

            # Landing guidance: cordon the nodes the ceilings exclude plus the source node, so the
            # scheduler has to pick the target. Wrapped in try/finally: exceptions, SIGTERM and a
            # rejected eviction all release the locks
            cordoned = []
            st = newnode = newpod = None
            try:
                if CFG["cordon_guide"] and (CFG["cordon_guide_sts"] or not c.get("sts")):
                    cordoned = cordon_bad_targets(k8s, src, t, scope, pod=pod, sts=bool(c.get("sts")))
                    if cordoned:
                        report_line("cordon-guide", target=t.name, cordoned=cordoned,
                                    why="temporarily cordoning non-target nodes so the scheduler "
                                        "cannot drop the pod on a full or memory-heavy node; "
                                        "released right after settle")

                code, body = k8s.evict(ns, name, pod["metadata"]["uid"])
                evict_ts = time.time()
                if code not in (200, 201, 202):
                    tried.add(pod["metadata"]["uid"])
                    rec["action"] = "evict-denied"
                    rec["code"] = code
                    rec["why"] = body.get("message", "")[:120]
                    report_line("skip", **rec)
                    summary["skipped"].append(rec)
                    continue
                if c.get("sts"):
                    st, newnode, newpod = wait_settle_sts(k8s, ns, name, pod["metadata"]["uid"],
                                                          src.name,
                                                          max(CFG["settle_timeout"], CFG["sts_settle_timeout"]))
                else:
                    _, rsuid = deployment_name(pod)
                    st, newnode, newpod = wait_settle(k8s, ns, rsuid, pod["metadata"]["uid"], src.name,
                                                      CFG["settle_timeout"], since_ts=evict_ts)
            finally:
                if cordoned:
                    uncordon_all(k8s, cordoned)
                    report_line("cordon-release", nodes=cordoned, reason="finally")
            if st is None:          # eviction rejected or an early exit: no result to record
                continue
            rec.update({"result": st, "landed": newnode, "new_pod": newpod})
            if st == "ok" and newnode and newnode != src.name:
                rec["action"] = "moved"
                quota["cycle"] -= 1
                quota["daily"] -= 1
                quota["per_ns"][ns] = quota["per_ns"].get(ns, 0) + 1
                if not c["multi"]:
                    quota["single"] -= 1
                if c.get("sts"):
                    quota["sts"] -= 1
                state["moves"].append(now_ts())
                state["cooldowns"][fp] = now_ts() + (CFG["sts_cooldown_hours"] if c.get("sts") else CFG["cooldown_hours"]) * 3600
                plan_count[newnode] = plan_count.get(newnode, 0) + 1
            elif st == "ok":
                rec["action"] = "bounce"
                # The eviction already happened (the pod restarted), so quota must be consumed; otherwise a
                # single round retries the same workload up to max_per_node times
                quota["cycle"] -= 1
                quota["daily"] -= 1
                quota["per_ns"][ns] = quota["per_ns"].get(ns, 0) + 1
                if not c["multi"]:
                    quota["single"] -= 1
                if c.get("sts"):
                    quota["sts"] -= 1
                state["moves"].append(now_ts())
                state["cooldowns"][fp] = now_ts() + (CFG["sts_cooldown_hours"] if c.get("sts") else CFG["cooldown_hours"]) * 3600
            elif st == "timeout":
                # This is "not up yet", not "failed": the eviction happened and the pod was healthy before.
                # Record it as slow (quota consumed, cooldown set) and continue the round -- one slow start
                # must not freeze the whole balancer
                rec["action"] = "slow"
                quota["cycle"] -= 1
                quota["daily"] -= 1
                quota["per_ns"][ns] = quota["per_ns"].get(ns, 0) + 1
                if not c["multi"]:
                    quota["single"] -= 1
                if c.get("sts"):
                    quota["sts"] -= 1
                state["moves"].append(now_ts())
                state["cooldowns"][fp] = now_ts() + (CFG["sts_cooldown_hours"] if c.get("sts") else CFG["cooldown_hours"]) * 3600
                report_line("move", **rec)
                summary["moves"].append(rec)
                report_line("slow-settle", pod=f"{ns}/{name}", timeout=CFG["settle_timeout"],
                            note="no Ready replacement before the timeout; the pod may still be rebuilding. "
                         "Cooled down, round continues")
                continue
            elif st in CRASH_REASONS and c.get("pre_bad"):
                # The workload was already crashing before the move (another replica in CrashLoop or with
                # many restarts); this is not caused by the migration
                rec["action"] = "pre-existing"
                report_line("move", **rec)
                report_line("pre-existing-crash", pod=f"{ns}/{name}", state=st,
                            note="the workload was already unhealthy before the move: cooled down only, "
                            "round continues, no downgrade")
                summary["moves"].append(rec)
                state["cooldowns"][fp] = now_ts() + CFG["cooldown_hours"] * 3600
                continue
            else:  # it really failed (ImagePull / CrashLoop): stop now and downgrade to dry-run
                rec["action"] = "failed"
                report_line("move", **rec)
                summary["moves"].append(rec)
                report_line("abort", why=f"{ns}/{name} -> {st}; stopping the rest of this round")
                save_state(k8s, state, state_cm, {**summary, "aborted": True})
                if CFG["auto_downgrade"]:
                    code_cm = set_mode(k8s, "dry-run")
                    report_line("auto-downgrade", to="dry-run", configmap="/".join(CFG["cfg_cm"]),
                                http=code_cm,
                                why="a moved pod failed to start; MODE was set back to dry-run, later rounds are read-only")
                notify(state, "abort", [
                    f"**Cluster**: {CFG['cluster_name']}  **Mode**: {CFG['mode']}",
                    f"**Incident**: a moved pod failed to start -- `{ns}/{name}` -> **{st}**",
                    f"- source: `{src.name}` (real CPU {src.act_cpu_pct:.0f}%, slots {src.pod_pct:.0f}%)",
                    f"- target: `{newnode or '?'}`  new pod: `{newpod or '?'}`",
                    "- the round was **stopped immediately** and `MODE` was set back to `dry-run`",
                    "- check the workload image and events, then set `MODE` back to `live` by hand",
                ], force=True)
                return 2
            if cordoned:
                uncordon_all(k8s, cordoned)
                report_line("cordon-release", nodes=cordoned, reason="settle-done")
            if CFG["landing_guard"] and newnode and newnode != src.name:
                g = check_landing(k8s, newnode)
                v = landing_guard_violation(g)
                if v:
                    rec["landed_out_of_guard"] = {"reason": v, **(g or {})}
                    state["cooldowns"][fp] = now_ts() + (CFG["sts_cooldown_hours"] if c.get("sts") else CFG["cooldown_hours"]) * 3600
                    report_line("landed-out-of-guard", pod=f"{ns}/{name}", landed=newnode,
                                reason=v, guard=g,
                                why="the scheduler landed the pod outside our ceilings (it only reads request percentages); "
                                    "the workload is cooled down; stop sending it to this node")
            report_line("move", **rec)
            summary["moves"].append(rec)
            n_moved += 1
            # Re-measure the waterlines (live mode advances on real data)
            nodes, rs_replicas = build_world(k8s, state)
            scope = in_scope(nodes)
            refresh_pool_used(scope)
            src = scope.get(src.name, src)
            targets = pick_targets(src, scope)
            if not targets:
                break
        if n_moved == 0:
            report_line("idle", src=src.name, why="no movable pod (filters, cooldown or quota)",
                        candidates=explain(src, rs_replicas, nodes, state))

    # ---------------- out of capacity -> notify (only when the cluster really has no room)
    caps = []
    for pool, st_ in summary["pool"].items():
        if st_["free_pct"] < CFG["pool_min_free_pct"]:
            caps.append(f"- pool `{pool}`: **{st_['free_slots']}** free slots ({st_['free_pct']}%), effectively full")
    stuck = [s for s in summary["skipped"] if s.get("why") in ("no-target", "pool-saturated")]
    moved = len(summary["moves"])
    if CFG["mode"] == "live" and srcs and moved == 0 and (caps or stuck):
        hot = sorted(scope.values(), key=lambda n: -max(n.act_cpu_pct, n.act_mem_pct, n.pod_pct))[:3]
        guard_bad = len([m for m in summary["moves"] if m.get("landed_out_of_guard")])
        lines = [
            f"**Cluster**: {CFG['cluster_name']}  **Mode**: {CFG['mode']}",
            f"**Conclusion**: not enough capacity to keep balancing -- {len(srcs)} nodes over a waterline, "
                 f"0 moves this round",
            "",
            "**Capacity**",
        ] + (caps or ["- no pool is full, yet no node could take the pod (see below)"]) + [
            "",
            "**Hottest nodes (real / slots)**",
        ] + [f"- `{n.name}` real CPU {n.act_cpu_pct:.0f}%, real memory {n.act_mem_pct:.0f}%, "
                                        f"slots {n.pod_pct:.0f}%" for n in hot] + [
            "",
            f"**Stuck sources**: {len(stuck)} (no-target / pool-saturated)",
            f"**Landings outside the ceilings**: {guard_bad}",
            "",
            "**Next step**: add nodes to the fullest pool (more pod slots), or loosen `DENY_WORKLOADS` / "
                 "the quotas; balancing resumes automatically once capacity is available",
        ]
        notify(state, "capacity", lines)

    # ---------------- per-node CPU/memory alert (a hard limit: move it or say so)
    hot_nodes = [n for n in scope.values()
                 if n.act_mem_phys_pct >= CFG["mem_act_high"] or n.act_cpu_pct >= CFG["cpu_act_high"]]
    if hot_nodes:
        moved_by = {}
        for m in summary["moves"]:
            if m.get("action") in ("moved", "slow"):
                moved_by[m["src"]] = moved_by.get(m["src"], 0) + 1
        stuck_by = {s_.get("src"): s_.get("why") for s_ in summary["skipped"] if s_.get("src")}
        lines = [f"**Cluster**: {CFG['cluster_name']}  **Mode**: {CFG['mode']}", "",
                 f"**Node over its waterline** (physical memory >= {CFG['mem_act_high']:.0f}% or real CPU "
                 f">= {CFG['cpu_act_high']:.0f}%)", ""]
        for n in sorted(hot_nodes, key=lambda x: -max(x.act_mem_phys_pct, x.act_cpu_pct))[:8]:
            did = moved_by.get(n.name, 0)
            state_txt = (f"{did} pod(s) moved this round" if did
                     else f"nothing moved ({stuck_by.get(n.name, 'not reached this round')})")
            lines.append(f"- `{n.name}` ({n.pool_name}) physical memory {n.act_mem_phys_pct:.0f}% "
                         f"real CPU {n.act_cpu_pct:.0f}%, slots {n.pod_pct:.0f}% -- {state_txt}")
        lines += ["", f"**Moves this round**: {len(summary['moves'])}  **Pending pods**: {PENDING['n']}"]
        report_line("node-hot", detail=[{"node": n.name, "pool": n.pool_name,
                                         "mem_phys": round(n.act_mem_phys_pct, 1),
                                         "cpu": round(n.act_cpu_pct, 1),
                                         "slot": round(n.pod_pct, 1),
                                         "moved": moved_by.get(n.name, 0),
                                         "stuck": stuck_by.get(n.name)} for n in hot_nodes][:12])
        notify(state, "node-hot", lines)

    # ---------------- pool-level slot alert (a single node at 95% is not urgent; a pool is)
    over = []
    for pool, st_ in summary["pool"].items():
        if st_.get("slot_used_pct", 0) > CFG["pool_slot_notify"]:
            over.append(st_ | {"pool": pool})
    if over:
        over.sort(key=lambda x: -x["slot_used_pct"])
        lines = [f"**Cluster**: {CFG['cluster_name']}  **Mode**: {CFG['mode']}", "",
                 f"**Pool slot usage above {CFG['pool_slot_notify']:.0f}%**", ""]
        for o in over:
            lines.append(f"- `{o['pool']}`: **{o['slot_used_pct']:.1f}%**"
                         f"({o['used_slots']}/{o['cap_slots']}, {o['free_slots']} free)")
        top = sorted([n for n in scope.values() if n.pool_name in {o['pool'] for o in over}],
                     key=lambda n: -n.pod_pct)[:5]
        lines += ["", "**Fullest nodes**"] + [
            f"- `{n.name}` slots {n.pod_pct:.0f}% ({len(n.pods)}/{n.alloc_pods}), physical memory "
                                        f"{n.act_mem_phys_pct:.0f}%" for n in top]
        lines += ["", f"**Moves this round**: {len(summary['moves'])}  **Pending pods**: {PENDING['n']}",
                  "", "**Next step**: missing slots is a capacity problem -- add nodes to this pool or "
                 "reduce redundant replicas"]
        report_line("pool-slot-high", detail=[{k: o[k] for k in ("pool", "slot_used_pct", "used_slots", "cap_slots")} for o in over])
        notify(state, "pool-slot", lines)
    else:
        report_line("pool-slot-ok", threshold=CFG["pool_slot_notify"],
                    detail={p: st_.get("slot_used_pct") for p, st_ in summary["pool"].items()})

    # Always persist state: the EMA needs samples collected during dry-run too. This only writes
    # our own ConfigMap and never touches workloads.
    code = save_state(k8s, state, state_cm, summary)
    report_line("summary", state_written=code, no_metrics=NO_METRICS["n"], pending=PENDING["n"],
                **{k: summary[k] for k in ("pool",)})
    report_line("done", moves=len(summary["moves"]), skipped=len(summary["skipped"]),
                no_metrics=NO_METRICS["n"], pending=PENDING["n"])
    return 0

if __name__ == "__main__":
    sys.exit(main())
