# node-leveler

**English** · [简体中文](#简体中文)

A small, self-contained Kubernetes **node waterline balancer**.

It runs as a CronJob, looks at how full each node is (pod slots, CPU, memory),
and **evicts a handful of pods per round** so the scheduler re-places them on
less loaded nodes. That is the whole write surface: `pods/eviction` plus a
short-lived `cordon` while a pod is being re-placed.

```
$ kubectl -n cluster-ops logs job/node-leveler-29896290
{"kind":"start","mode":"live","nodes":18,"pools":{"general/4C":{"slot_used_pct":93.5}}}
{"kind":"sources","count":7,"detail":[{"node":"node-a","driver":"pool_slot","value":93.5}]}
{"kind":"move","pod":"demo/app-7d9f-abc","src":"node-a","landed":"node-k","action":"moved"}
{"kind":"done","moves":4,"pending":0}
```

## Why

`kube-scheduler` only looks at **requests** when placing a pod, and only at
placement time. In practice:

* requests are often set far below real usage (we have seen pods running at
  11× their request), and
* over time everything drifts: a pool of small nodes fills up to 95%+ while
  large nodes next to them sit at 70%.

This project attacks both halves of that: balance on what the nodes **actually
use**, and verify that an evicted pod **actually landed** somewhere better.
It is deliberately small — **one Python file, no dependencies, no DaemonSet,
every rule a ConfigMap key, `dry-run` by default.**

## How it decides

Three gates, all configurable, nothing hard-coded:

1. **Source** — a node becomes a candidate when its slot / CPU / memory
   waterline crosses a threshold, or when its whole *pool* is over a waterline
   (`POOL_SLOT_PRESSURE`) even if no single node is.
2. **Target** — every candidate node is tried pod-by-pod: `nodeSelector` →
   `nodeAffinity` → taints/tolerations → slot/CPU/memory fit → *does the target
   stay under its ceilings after the move* → *is it still better than the
   source after the move*. Cross-machine-size moves (4C → 16C) are allowed; the
   pod's own placement rules decide, not the pool name.
3. **Quota** — per round / per node / per namespace / per single-replica
   workload / per StatefulSet / per day, plus a per-workload cooldown. A round
   that would exceed any of them simply does less.

The driver dimension is chosen per node: **pod slots first, then real CPU/memory,
then requests**. Real usage comes from `metrics.k8s.io` with EMA smoothing, and
it doubles as the OOM guard (`PHYS_AFTER_MEM_MAX`).

## Safety model

| Concern | Behaviour |
|---|---|
| Accidentally deleting pods | It never calls `DELETE /pods`. Eviction API only, so PodDisruptionBudgets apply. |
| Uncontrolled placement | While a pod is being replaced, the other eligible nodes are briefly cordoned so the scheduler lands it where we computed. Always released in a `finally` block, and stale labels are healed by TTL on the next round. |
| Runaway migration | Hard caps per round/node/ns/day, single-replica quota, 6 h cooldown per workload, 20 min minimum pod age. |
| Broken workloads | A workload that was already crash-looping before the move is only cooled down, never allowed to abort the whole round. |
| Registry outage | Set `MODE=off` or `suspend: true`; never migrate while nodes cannot pull images. |
| Unknown real usage | Pods without metrics are skipped (`ACT_MISSING_POLICY=skip`) instead of being treated as zero. |
| Default state | Chart ships with `MODE=dry-run` and `suspend: true`. Nothing happens until you say so. |

`MODE=live` is the only state that evicts. Start in `dry-run` for a few days:
every round logs the plan (`sources`, `plan`) without touching anything.

## Testing

Runtime dependencies: none (standard library only). Tests are dev-only.

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest -q          # ~135 tests, well under a second
```

What is covered without a cluster:

* **Decision logic** — `driver()`, `headroom_ok()`, `fits()`, `choose_move()`:
  every ceiling, the source waterlines, the pool-pressure driver, quota gates,
  cooldowns and the StatefulSet zone rule.
* **Regression cases** — each behaviour that broke in production has a test: the
  cross-node-shape percentage trap, the single-replica quota, missing metrics,
  Pending-pod counting, pagination of the pod list.
* **Chart drift** — `tests/test_chart_defaults.py` asserts that the values shipped
  in `chart/values.yaml` match the code defaults, so a local `python3 balancer.py`
  cannot behave differently from the CronJob.

CI runs the same suite on every push and pull request, alongside `helm lint` and a
check that the rendered CronJob stays `suspend: true` / `MODE: dry-run`.

## Install

```bash
helm install node-leveler ./chart \
  -f examples/values-small-cluster.yaml \
  -n cluster-ops --create-namespace

# look at what it would do
kubectl -n cluster-ops create job --from=cronjob/node-leveler manual-$(date +%H%M)

# turn it on
helm upgrade node-leveler ./chart -f examples/values-small-cluster.yaml -n cluster-ops \
  --set config.MODE=live --set suspend=false
```

Image: `ghcr.io/concurrent2024/node-leveler` (multi-arch, built by CI on tag).

## Configure

Everything is a key in the chart's values → rendered into one ConfigMap →
read as environment variables. See **[docs/configuration.md](docs/configuration.md)**
for the full list, and `examples/` for a starting point.

The ones you almost certainly need to touch:

| Key | Meaning |
|---|---|
| `POOLS` / `POOL_LABEL` | which node pools to scan (`<label value>/<CPU>`), and which node label defines a pool |
| `NS_INCLUDE` / `NS_EXCLUDE` / `DENY_WORKLOADS` | which workloads may be moved |
| `MODE` | `dry-run` / `live` / `off` |
| `CLUSTER_NAME` | shown in notifications only |

## Notifications (optional)

DingTalk webhook. Credentials come from a Secret named `node-leveler-notify`
(`DINGTALK_TOKEN`, `DINGTALK_SECRET`); the CronJob references it as
`optional: true`, so a missing Secret is never fatal — alerts are just skipped.

Two alert classes, both rate-limited:

* `node-hot` — a single node is over its memory/CPU waterline (must move or tell you).
* `pool-slot-high` — a whole pool's slot usage crossed the line.

## When not to use this

* You need a plugin-extensible framework with a community and an e2e suite —
  this is a 1.5k-line single file, not a framework.
* Your nodes have no hard pod-count limit and requests are accurate — a plain
  requests-based balancing pass is enough, and this adds no value over it.
* You have fewer than ~5 nodes per pool: there is no waterline to level, and every
  restart costs more than the imbalance does.
* You need scheduling-time decisions (place new pods on idle nodes) — that is a
  scheduler plugin's job, not a CronJob's.
* You are on a cloud autoscaler where node count itself is the cost knob —
  node consolidation (bin-pack, then remove nodes) is the right tool.
* You cannot tolerate any single-replica restart: this tool would have to be
  restricted so heavily (`MAX_SINGLE_REPLICA=0`, `MAX_STS_PER_CYCLE=0`) that only
  multi-replica Deployments move.

## Limitations — read before enabling on production

* Placement is **influenced, not guaranteed**. The scheduler still decides; the
  balancer only improves the odds and then verifies the landing.
* One move = one real restart. Single-replica workloads are capped separately
  and should be excluded unless you accept the blip.
* Stateful workloads can only move within the same zone (`ebs`/`local` PV
  affinity). The chart defaults to `MAX_STS_PER_CYCLE=0`.
* It does not fix inflated requests. If a pod asks for 256 Mi and uses 3 Gi,
  moving it just relocates the problem — fix the request or the node size.
* `AUTO_DOWNGRADE=true` will rewrite the ConfigMap and set `MODE=dry-run` after a
  migration-related failure. It does **not** recover by itself.

## Operations

```bash
# what would it do right now (no writes, no state change)
kubectl -n cluster-ops create job --from=cronjob/node-leveler manual-$(date +%H%M)

# stop it
helm upgrade node-leveler ./chart -f <values> -n cluster-ops --set suspend=true

# inspect state (cooldowns, EMA samples, single-instance lock)
kubectl -n cluster-ops get cm node-leveler-state -o jsonpath='{.data.state}'

# history of rounds, from Loki (Job logs are TTL'd)
python3 scripts/rounds.py --hours 24
```

The state ConfigMap is deliberately **not** part of the Helm release: `helm
uninstall` will not delete it, so cooldowns survive a reinstall.

Both the parameter and the state ConfigMap default to the namespace the balancer
runs in (`STATE_CM` / `CFG_CM` empty), so installing it anywhere keeps its state
there without extra configuration. Set them explicitly only when state should live
somewhere else.

## License

MIT — see [LICENSE](LICENSE).

---

## 简体中文

一个很小的、自包含的 Kubernetes **节点水位均衡器**。

它以 CronJob 的形式运行：看每台节点有多满（pod 槽位、CPU、内存），然后**每轮驱逐少量 pod**，
让调度器把它们重新放到更空闲的节点上。写入面就这些：`pods/eviction`，以及 pod 重建期间短暂的 `cordon`。

```
$ kubectl -n cluster-ops logs job/node-leveler-29896290
{"kind":"start","mode":"live","nodes":18,"pools":{"general/4C":{"slot_used_pct":93.5}}}
{"kind":"sources","count":7,"detail":[{"node":"node-a","driver":"pool_slot","value":93.5}]}
{"kind":"move","pod":"demo/app-7d9f-abc","src":"node-a","landed":"node-k","action":"moved"}
{"kind":"done","moves":4,"pending":0}
```

### 为什么需要它

`kube-scheduler` 在放置 pod 时**只看 requests**，而且只在放置那一刻看。实际中：

* requests 常常被写得远低于真实用量（我们见过跑在 11 倍 request 上的 pod）；
* 时间一长分布就歪了：一池小节点填到 95%+，旁边的大节点才 70%。

本项目专门解决这两半：按节点**实际用量**来均衡，并**核实**被驱逐的 pod 是否真的落到了更好的位置。
它刻意做得很小 —— **一个 Python 文件、零依赖、不装 DaemonSet、所有规则都是 ConfigMap 键、默认 dry-run。**

### 怎么决策

三道闸门，全部可配，没有硬编码：

1. **源** —— 节点的槽位 / CPU / 内存越过水位线就入选；或者它所在的**整个池**超过了水位线
   （`POOL_SLOT_PRESSURE`），即使没有任何单节点越线。
2. **目标** —— 逐个候选节点、逐个 pod 试放：`nodeSelector` → `nodeAffinity` → 污点/容忍 →
   槽位/CPU/内存放得下 → **放完之后目标不越过自己的天花板** → **放完之后仍然比源更好**。
   允许跨机型（4C → 16C）迁移，由 pod 自己的放置规则决定，而不是池名。
3. **配额** —— 每轮 / 每节点 / 每命名空间 / 每单副本工作负载 / 每 StatefulSet / 每天，外加
   单个工作负载的冷却时间。超限就少做几个，不会硬来。

驱动维度按节点自动选：**pod 槽位优先，其次实际 CPU/内存，最后 requests**。实际用量取自
`metrics.k8s.io` 并做 EMA 平滑，同时兼作 OOM 护栏（`PHYS_AFTER_MEM_MAX`）。

### 安全模型

| 担心的事 | 行为 |
|---|---|
| 误删 pod | 从不调用 `DELETE /pods`。只用 Eviction API，因此 PDB 生效 |
| 落点不受控 | pod 重建期间临时 cordon 其它合格节点，让调度器落到算好的位置；全程 `finally` 释放，遗留 label 由 TTL 自愈 |
| 迁移失控 | 每轮/每节点/每 ns/每天硬上限，单副本独立配额，同一工作负载 6 小时冷却，pod 最小年龄 20 分钟 |
| 本来就坏的工作负载 | 迁移前已在崩溃循环的，只做冷却，不允许它中止整轮 |
| 镜像仓库故障 | 设 `MODE=off` 或 `suspend: true`；节点拉不到镜像时绝不迁移 |
| 拿不到实际用量 | 没有 metrics 的 pod 直接跳过（`ACT_MISSING_POLICY=skip`），不当成 0 |
| 默认状态 | chart 默认 `MODE=dry-run` + `suspend: true`，没人点头就什么都不做 |

只有 `MODE=live` 才会驱逐。建议先跑几天 `dry-run`：每轮日志会打印计划（`sources`、`plan`），
但不动任何东西。

### 安装

```bash
helm install node-leveler ./chart \
  -f examples/values-small-cluster.yaml \
  -n cluster-ops --create-namespace

# 看它打算做什么
kubectl -n cluster-ops create job --from=cronjob/node-leveler manual-$(date +%H%M)

# 真正开启
helm upgrade node-leveler ./chart -f examples/values-small-cluster.yaml -n cluster-ops \
  --set config.MODE=live --set suspend=false
```

镜像：`ghcr.io/concurrent2024/node-leveler`（多架构，打 tag 时由 CI 构建）。

### 配置

全部是 chart values 里的键 → 渲染成一个 ConfigMap → 以环境变量读取。
完整清单见 **[docs/configuration.md](docs/configuration.md)**，起点见 `examples/`。

几乎一定要改的几个：

| 键 | 含义 |
|---|---|
| `POOLS` / `POOL_LABEL` | 扫描哪些池（`<标签值>/<CPU 核数>`），以及哪个节点标签定义池 |
| `NS_INCLUDE` / `NS_EXCLUDE` / `DENY_WORKLOADS` | 哪些工作负载可以被移动 |
| `MODE` | `dry-run` / `live` / `off` |
| `CLUSTER_NAME` | 仅用于通知文案 |

### 通知（可选）

钉钉 webhook。凭据来自名为 `node-leveler-notify` 的 Secret（`DINGTALK_TOKEN`、`DINGTALK_SECRET`）；
CronJob 以 `optional: true` 引用，Secret 不存在不会导致失败，只是不发通知。

两类告警，各自限流：

* `node-hot` —— 某台单节点的内存/CPU 越过水位线（必须迁移，或者通知你）；
* `pool-slot-high` —— 整个池的槽位占用越过水位线。

### 什么情况下不该用它

* 你需要一个可插件扩展、有社区和 e2e 的框架 —— 这是一份 1500 行的单文件，不是框架。
* 节点没有 pod 数量硬上限，且 requests 写得准 —— 普通的水位均衡就够了，不必引入新习惯。
* 每个池少于约 5 台节点：没有水位可调平，每次重启的代价大于不均衡。
* 你需要在**调度那一刻**做决策（把新 pod 放到闲节点）—— 那是调度器插件的活，不是 CronJob 的。
* 你在按节点数计费的云上 —— 节点收敛（先装箱、再删节点）才是对的工具。
* 完全不能容忍任何单副本重启：只能把配额压到 `MAX_SINGLE_REPLICA=0`、`MAX_STS_PER_CYCLE=0`，
  那就只剩多副本 Deployment 能动。

### 局限 —— 上生产前先读

* 落点是**影响**，不是**保证**。最终仍由调度器决定；本工具只提高概率，并在落地后复核。
* 一次迁移 = 一次真实重启。单副本工作负载单独配额，除非接受闪断，否则应排除。
* 有状态负载只能在同一 zone 内移动（`ebs`/`local` PV 亲和）。chart 默认 `MAX_STS_PER_CYCLE=0`。
* 它不修“requests 虚高/虚低”。如果 pod 申请 256 Mi 却用 3 Gi，搬它只是把问题挪个位置 ——
  该改 request 或者换机型。
* `AUTO_DOWNGRADE=true` 会在迁移相关失败后改写 ConfigMap，把 `MODE` 设回 `dry-run`；
  它**不会**自己恢复。

### 运维

```bash
# 现在它会做什么（不写入、不改状态）
kubectl -n cluster-ops create job --from=cronjob/node-leveler manual-$(date +%H%M)

# 停掉
helm upgrade node-leveler ./chart -f <values> -n cluster-ops --set suspend=true

# 查看状态（冷却表、EMA 样本、单实例锁）
kubectl -n cluster-ops get cm node-leveler-state -o jsonpath='{.data.state}'

# 从 Loki 查历史轮次（Job 日志有 TTL）
python3 scripts/rounds.py --hours 24
```

状态 ConfigMap 刻意**不属于** Helm release：`helm uninstall` 不会删它，所以重装不会丢冷却记录。

### 许可

MIT，见 [LICENSE](LICENSE)。
