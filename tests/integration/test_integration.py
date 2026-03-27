"""
Integration tests for cdp-mcp-server using WireMock containers.

Prerequisites:
  docker compose -f tests/integration/docker-compose.yml up -d

Run:
  REGISTRY_BACKEND=file REGISTRY_FILE_PATH=tests/integration/cm_instances.yaml \
    .venv/bin/pytest tests/integration/ -v -m integration

These tests require the WireMock containers to be running.
They are skipped automatically if the CM mock is not reachable.
"""
from __future__ import annotations

import os
import socket
import pytest

import httpx

from cdp_mcp.clients.yarn_client import YarnClient
from cdp_mcp.clients.hdfs_client import HdfsClient
from cdp_mcp.registry.file_registry import FileRegistry


# ── Markers ──────────────────────────────────────────────────────────────────

def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


requires_docker = pytest.mark.skipif(
    not _port_open("localhost", 7183),
    reason="WireMock CM container not running (start with docker compose up -d)",
)

pytestmark = pytest.mark.integration


# ── FileRegistry ──────────────────────────────────────────────────────────────

@requires_docker
def test_file_registry_loads_instances():
    """FileRegistry should load instances from the integration YAML."""
    yaml_path = os.path.join(os.path.dirname(__file__), "cm_instances.yaml")
    reg = FileRegistry(yaml_path)
    instances = reg.load()
    assert len(instances) >= 1
    assert instances[0].host == "localhost"
    assert instances[0].port == 7183


# ── YARN integration ──────────────────────────────────────────────────────────

@requires_docker
@pytest.mark.asyncio
@pytest.mark.skipif(not _port_open("localhost", 8088), reason="YARN mock not running")
async def test_yarn_list_apps():
    client = YarnClient("http://localhost:8088")
    apps = await client.list_apps()
    assert isinstance(apps, list)
    assert len(apps) >= 1
    assert apps[0]["app_id"] is not None


@requires_docker
@pytest.mark.asyncio
@pytest.mark.skipif(not _port_open("localhost", 8088), reason="YARN mock not running")
async def test_yarn_get_app_succeeded():
    client = YarnClient("http://localhost:8088")
    result = await client.get_app("application_1234567890_0001")
    assert result["final_status"] == "SUCCEEDED"
    assert result["user"] == "alice"


@requires_docker
@pytest.mark.asyncio
@pytest.mark.skipif(not _port_open("localhost", 8088), reason="YARN mock not running")
async def test_yarn_get_app_failed_has_diagnostics():
    client = YarnClient("http://localhost:8088")
    result = await client.get_app("application_failed_0001")
    assert result["final_status"] == "FAILED"
    assert result["diagnostics"]  # should not be empty


@requires_docker
@pytest.mark.asyncio
@pytest.mark.skipif(not _port_open("localhost", 8088), reason="YARN mock not running")
async def test_yarn_get_queue():
    client = YarnClient("http://localhost:8088")
    result = await client.get_queue()
    assert result["name"] == "root"
    assert result["capacity"] == 100.0


# ── HDFS integration ──────────────────────────────────────────────────────────

@requires_docker
@pytest.mark.asyncio
@pytest.mark.skipif(not _port_open("localhost", 9870), reason="HDFS mock not running")
async def test_hdfs_namenode_status():
    client = HdfsClient("http://localhost:9870")
    result = await client.get_namenode_status()
    assert result["health_summary"] == "HEALTHY"
    assert result["safe_mode"] is False
    assert result["capacity_total_gb"] > 0
    assert result["active_namenode"] is not None
