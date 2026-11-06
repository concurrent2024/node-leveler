"""The chart is the shipped configuration; the code defaults are only a fallback.

These two drifted once already (the chart shipped `target_memuse_max=90` while the
code default was 75), which made a local `python3 balancer.py` behave differently
from the CronJob. This test fails when they disagree again.
"""
from pathlib import Path

import pytest
import yaml

import balancer as B
from conftest import CODE_DEFAULTS

VALUES = Path(__file__).resolve().parent.parent / "chart" / "values.yaml"

# keys whose ConfigMap value is parsed into a different Python type
SKIP = {"pools", "deny_workloads", "allow_workloads", "ns_exclude"}


def _coerce(raw, ref):
    if isinstance(ref, bool):
        return str(raw).lower() == "true"
    if isinstance(ref, float):
        return float(raw)
    if isinstance(ref, int):
        return int(raw)
    return str(raw)


@pytest.mark.parametrize("key,raw", sorted((yaml.safe_load(VALUES.read_text())["config"]).items()))
def test_chart_default_matches_code_default(key, raw):
    cfg_key = key.lower()
    if cfg_key in SKIP or cfg_key not in CODE_DEFAULTS:
        pytest.skip(f"{key} is not a scalar setting")
    ref = CODE_DEFAULTS[cfg_key]
    if isinstance(ref, (set, list, dict, tuple)):
        pytest.skip(f"{key} is a collection")
    assert _coerce(raw, ref) == ref, (
        f"chart/values.yaml ships {key}={raw!r} but the code default is {ref!r}; "
        "align them so a local run behaves like the CronJob"
    )
