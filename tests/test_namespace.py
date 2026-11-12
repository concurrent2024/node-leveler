"""ConfigMap references default to the namespace the balancer runs in.

A deployment must not need to hard-code a namespace: install it anywhere and it keeps
its parameter and state ConfigMaps there.
"""
import importlib

import balancer as B


def _with_env(monkeypatch, **env):
    for key in ("POD_NAMESPACE", "NAMESPACE", "STATE_CM", "CFG_CM"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return B


def test_default_state_ref_is_our_own_namespace(monkeypatch):
    _with_env(monkeypatch, POD_NAMESPACE="team-ops")
    assert B._cm_ref("STATE_CM", "node-leveler-state") == ("team-ops", "node-leveler-state")


def test_namespace_env_falls_back_to_nam(monkeypatch):
    _with_env(monkeypatch, NAMESPACE="second-choice")
    assert B._cm_ref("STATE_CM", "node-leveler-state")[0] == "second-choice"


def test_explicit_ns_name_ref_wins(monkeypatch):
    _with_env(monkeypatch, POD_NAMESPACE="ignored", STATE_CM="other-ns/custom-state")
    assert B._cm_ref("STATE_CM", "node-leveler-state") == ("other-ns", "custom-state")


def test_bare_name_uses_own_namespace(monkeypatch):
    _with_env(monkeypatch, POD_NAMESPACE="team-ops", CFG_CM="only-a-name")
    assert B._cm_ref("CFG_CM", "node-leveler-config") == ("team-ops", "only-a-name")


def test_own_namespace_never_returns_empty(monkeypatch):
    _with_env(monkeypatch)
    assert B.own_namespace()             # in-cluster file or "default"
