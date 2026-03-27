"""Unit tests for EnvRegistry."""
from __future__ import annotations

import pytest

from cdp_mcp.registry.env_registry import EnvRegistry


def test_load_single_instance(monkeypatch):
    monkeypatch.setenv("CM_HOST", "cm.example.com")
    monkeypatch.setenv("CM_PORT", "7183")
    monkeypatch.setenv("CM_USERNAME", "admin")
    monkeypatch.setenv("CM_PASSWORD", "secret")
    monkeypatch.setenv("CM_ENVIRONMENT", "prod")
    monkeypatch.setenv("CM_USE_TLS", "false")
    monkeypatch.setenv("CM_VERIFY_SSL", "true")
    monkeypatch.setenv("CM_API_VERSION", "v51")

    reg = EnvRegistry()
    instances = reg.load()

    assert len(instances) == 1
    inst = instances[0]
    assert inst.host == "cm.example.com"
    assert inst.port == 7183
    assert inst.username == "admin"
    assert inst.password == "secret"
    assert inst.environment_name == "prod"
    assert inst.use_tls is False
    assert inst.verify_ssl is True
    assert inst.api_version == "v51"


def test_load_no_host_returns_empty(monkeypatch):
    monkeypatch.delenv("CM_HOST", raising=False)
    reg = EnvRegistry()
    instances = reg.load()
    assert instances == []


def test_get_all_after_start(monkeypatch):
    monkeypatch.setenv("CM_HOST", "cm.test")
    monkeypatch.setenv("CM_USERNAME", "admin")
    monkeypatch.setenv("CM_PASSWORD", "pass")
    reg = EnvRegistry()
    reg.start()
    all_inst = reg.get_all()
    assert len(all_inst) == 1
    assert all_inst[0].host == "cm.test"


def test_defaults_applied(monkeypatch):
    monkeypatch.setenv("CM_HOST", "cm.test")
    monkeypatch.delenv("CM_PORT", raising=False)
    monkeypatch.delenv("CM_USERNAME", raising=False)
    monkeypatch.delenv("CM_PASSWORD", raising=False)
    monkeypatch.delenv("CM_ENVIRONMENT", raising=False)
    monkeypatch.delenv("CM_USE_TLS", raising=False)
    monkeypatch.delenv("CM_VERIFY_SSL", raising=False)
    monkeypatch.delenv("CM_API_VERSION", raising=False)

    reg = EnvRegistry()
    instances = reg.load()
    assert len(instances) == 1
    inst = instances[0]
    assert inst.port == 7183
    assert inst.username == "admin"
    assert inst.environment_name == "default"
    assert inst.use_tls is False
    assert inst.verify_ssl is True
    assert inst.api_version == "v51"


def test_list_raw(monkeypatch):
    monkeypatch.setenv("CM_HOST", "cm.example.com")
    monkeypatch.setenv("CM_USERNAME", "admin")
    monkeypatch.setenv("CM_PASSWORD", "secret")
    reg = EnvRegistry()
    reg.start()
    raw = reg.list_raw()
    assert len(raw) == 1
    assert "password" not in raw[0]
    assert raw[0]["host"] == "cm.example.com"


def test_get_stats(monkeypatch):
    monkeypatch.setenv("CM_HOST", "cm.example.com")
    monkeypatch.setenv("CM_USERNAME", "admin")
    monkeypatch.setenv("CM_PASSWORD", "secret")
    monkeypatch.setenv("CM_ENVIRONMENT", "test")
    reg = EnvRegistry()
    reg.start()
    stats = reg.get_stats()
    assert stats["backend"] == "env"
    assert stats["total"] == 1
    assert stats["active"] == 1


def test_register_raises(monkeypatch):
    monkeypatch.setenv("CM_HOST", "cm.example.com")
    monkeypatch.setenv("CM_USERNAME", "admin")
    monkeypatch.setenv("CM_PASSWORD", "secret")
    reg = EnvRegistry()
    reg.start()
    with pytest.raises(NotImplementedError, match="read-only"):
        reg.register(host="new.cm")


def test_deactivate_raises(monkeypatch):
    monkeypatch.setenv("CM_HOST", "cm.example.com")
    monkeypatch.setenv("CM_USERNAME", "admin")
    monkeypatch.setenv("CM_PASSWORD", "secret")
    reg = EnvRegistry()
    reg.start()
    with pytest.raises(NotImplementedError, match="read-only"):
        reg.deactivate("cm.example.com")


def test_update_field_raises(monkeypatch):
    monkeypatch.setenv("CM_HOST", "cm.example.com")
    monkeypatch.setenv("CM_USERNAME", "admin")
    monkeypatch.setenv("CM_PASSWORD", "secret")
    reg = EnvRegistry()
    reg.start()
    with pytest.raises(NotImplementedError, match="read-only"):
        reg.update_field("cm.example.com", "port", "7184")


def test_stop_is_noop(monkeypatch):
    monkeypatch.setenv("CM_HOST", "cm.example.com")
    monkeypatch.setenv("CM_USERNAME", "admin")
    monkeypatch.setenv("CM_PASSWORD", "secret")
    reg = EnvRegistry()
    reg.start()
    reg.stop()  # Should not raise
