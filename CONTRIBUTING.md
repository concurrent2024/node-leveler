# Contributing

## The one rule that matters: no real infrastructure in here

This repository is public. Examples, fixtures, docs and commit messages must never contain
real cluster names, real namespaces, real hostnames, real customer names or real email
addresses. A leak is invisible in review — `smtp.corp.example` reads like a
placeholder — so CI checks for it instead of relying on reviewers.

Use these placeholders everywhere:

| Real thing | Write this |
|---|---|
| cluster name | `prod-cluster`, `staging-cluster` |
| namespace | `team-a`, `team-b`, `app1` |
| workload | `orders-api`, `worker`, `db-0` |
| pool | `general/4C`, `general/16C`, `storage/8C` |
| mail server | `smtp.example.com` |
| registry | `ghcr.io/<owner>/node-leveler` |
| node | `node-a`, `node-b` |

## Before you push

```bash
python3 -m pytest -q     # 160 cluster-free tests, ~0.1s
helm lint ./chart
```

CI runs the same tests plus a hygiene check: private IP ranges, local filesystem paths and
non-`example.com` email addresses fail the build. A line that legitimately needs one of
those literals inside the check itself carries the marker `# noqa: hygiene`.

## Tests

Everything is standard library only — the CI job fails if a third-party import appears in
`balancer.py` or `scripts/rounds.py`. Tests may use `pytest`; the runtime may not.

## Publishing (maintainers)

Releases are cut by maintainers with an internal script that lives outside this repository:
it runs the test suite, the chart lint and the organisation's private vocabulary check, then
rewrites `main` to a single commit and moves the tag. Keep `main` a single commit — release
tags point at the released tree and the image tag equals the release tag. Contributors open a
PR instead of publishing.
