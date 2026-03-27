"""
server.py — FastMCP server entry point for cdp-mcp.
(Based on dvergari/cloudera-mcp-server, Apache 2.0)
"""
from __future__ import annotations

import json
import sys
from contextlib import asynccontextmanager
from typing import Any, Literal, Optional

import structlog
from mcp.server.fastmcp import FastMCP

from cdp_mcp.config import ServerSettings, build_registry
from cdp_mcp.cm_pool import CMPool
from cdp_mcp.clients.yarn_client import YarnClient, YarnNotFoundError, YarnClientError
from cdp_mcp.clients.spark_client import SparkClient, SparkNotFoundError, SparkClientError
from cdp_mcp.clients.hdfs_client import HdfsClient, HdfsClientError
from cdp_mcp.clients.oozie_client import OozieClient, OozieNotFoundError, OozieClientError

log = structlog.get_logger(__name__)

server_cfg = ServerSettings()
_registry = None
_pool: Optional[CMPool] = None


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def _lifespan(server):
    global _registry, _pool
    _registry = build_registry(server_cfg)
    _registry.start()
    instances = _registry.get_all()
    _pool = CMPool(instances, server_cfg)
    await _pool.start()
    log.info("cdp_mcp.ready", instances=len(instances))
    yield
    await _pool.stop()
    _registry.stop()


mcp = FastMCP("cdp-mcp", lifespan=_lifespan)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _dump(data: Any) -> str:
    return json.dumps(data, indent=2, default=str)


def _no_client(cluster_name: str) -> str:
    return _dump(
        {
            "error": (
                f"No Cloudera Manager found for cluster '{cluster_name}'. "
                "Use list_clusters() to see available clusters."
            )
        }
    )


# ── Original CM tools ─────────────────────────────────────────────────────────

@mcp.tool()
async def list_clusters() -> str:
    """
    List all CDP / Cloudera clusters managed by the configured CM instances.
    Returns cluster name, version, status and associated services.
    Use this as the starting point to discover available clusters.
    """
    results = []
    for env_name in _pool.list_environments():
        client = _pool.get_client_for_environment(env_name)
        if client is None:
            continue
        try:
            clusters = await client.list_clusters()
            results.extend(clusters)
        except Exception as exc:
            log.error("tool.list_clusters.error", env=env_name, error=str(exc))
            results.append({"error": str(exc), "environment": env_name})
    return _dump(results)


@mcp.tool()
async def list_services(cluster_name: str) -> str:
    """
    List all services running on a cluster.
    Returns service name, type, state and health status.

    Args:
      cluster_name: Cluster name as returned by list_clusters().
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(await client.list_services(cluster_name))
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_service_logs(
    cluster_name: str,
    service_name: str,
    max_lines: int = 500,
) -> str:
    """
    Retrieve recent log lines for all roles of a service.
    Returns a dict mapping role_name → list of log lines.
    Useful for diagnosing service failures.

    Args:
      cluster_name: Cluster name.
      service_name: Service name (e.g. YARN, SPARK_ON_YARN, HDFS, OOZIE).
      max_lines:    Maximum log lines per role (default 500).
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(
            await client.get_service_logs(cluster_name, service_name, max_lines)
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_alerts(
    cluster_name: str,
    category: Optional[str] = None,
    severity: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 50,
) -> str:
    """
    Get cluster alert events from Cloudera Manager.

    Args:
      cluster_name: Cluster name.
      category:     Event category filter (e.g. HEALTH_CHECK, LOG_MESSAGE).
      severity:     Severity filter (e.g. CRITICAL, WARNING, INFORMATIONAL).
      start_time:   ISO 8601 start time (default: 1 hour ago).
      end_time:     ISO 8601 end time (default: now).
      limit:        Maximum number of events to return (default 50).
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(
            await client.get_alerts(
                cluster_name,
                category=category,
                severity=severity,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
            )
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_service_metrics(
    cluster_name: str,
    service_name: str,
    metric_names: list[str],
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
) -> str:
    """
    Query time-series metrics for a service via the CM tsquery API.

    Args:
      cluster_name: Cluster name.
      service_name: Service name.
      metric_names: List of metric names (e.g. ["cpu_user_rate", "mem_rss"]).
      start_time:   ISO 8601 start time.
      end_time:     ISO 8601 end time.
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(
            await client.get_service_metrics(
                cluster_name,
                service_name,
                metric_names,
                start_time=start_time,
                end_time=end_time,
            )
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_config(
    cluster_name: str,
    service_name: str,
    view: str = "full",
) -> str:
    """
    Get the configuration of a service (all parameters with current values and defaults).

    Args:
      cluster_name: Cluster name.
      service_name: Service name.
      view:         "full" (all params) or "summary" (only explicitly set params).
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(await client.get_config(cluster_name, service_name, view=view))
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def update_config(
    cluster_name: str,
    service_name: str,
    configs: list[dict],
) -> str:
    """
    Update one or more configuration parameters for a service.
    Each item in configs must have 'name' and 'value' keys.

    Args:
      cluster_name: Cluster name.
      service_name: Service name.
      configs:      List of {"name": str, "value": str} dicts.

    WARNING: This is a write operation. Changes may require a service restart to take effect.
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(await client.update_config(cluster_name, service_name, configs))
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def run_service_command(
    cluster_name: str,
    service_name: str,
    command: str,
) -> str:
    """
    Execute a service-level command (e.g. restart, start, stop, refresh).
    Returns the command ID which can be polled with get_command_status().

    Args:
      cluster_name: Cluster name.
      service_name: Service name.
      command:      Command name (e.g. "restart", "start", "stop", "refresh").

    WARNING: This is a write operation that affects a running service.
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(
            await client.run_service_command(cluster_name, service_name, command)
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_command_status(command_id: int) -> str:
    """
    Check the status of an asynchronous CM command.
    Poll this after run_service_command() to know when it completes.

    Args:
      command_id: Command ID as returned by run_service_command().
    """
    # Use any available client for command status (commands are global)
    for env_name in _pool.list_environments():
        client = _pool.get_client_for_environment(env_name)
        if client:
            try:
                return _dump(await client.get_command_status(command_id))
            except Exception as exc:
                return _dump({"error": str(exc)})
    return _dump({"error": "No CM clients available."})


@mcp.tool()
async def get_host_status(
    cluster_name: Optional[str] = None,
    host_filter: Optional[str] = None,
) -> str:
    """
    Get health and role information for cluster hosts.

    Args:
      cluster_name: If set, return only hosts in this cluster.
      host_filter:  Optional CM filter expression (e.g. "hostname = myhost").
    """
    if cluster_name:
        client = _pool.get_client_for_cluster(cluster_name)
        if client is None:
            return _no_client(cluster_name)
        try:
            return _dump(
                await client.get_host_status(
                    cluster_name=cluster_name,
                    host_filter=host_filter,
                )
            )
        except Exception as exc:
            return _dump({"error": str(exc)})

    # No cluster specified: query all environments
    results = []
    for env_name in _pool.list_environments():
        client = _pool.get_client_for_environment(env_name)
        if client:
            try:
                results.extend(
                    await client.get_host_status(host_filter=host_filter)
                )
            except Exception as exc:
                results.append({"error": str(exc), "environment": env_name})
    return _dump(results)


@mcp.tool()
async def get_audit_events(
    cluster_name: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    service_name: Optional[str] = None,
    user_name: Optional[str] = None,
    limit: int = 50,
) -> str:
    """
    Retrieve CM audit events (login, config changes, command executions).

    Args:
      cluster_name: If set, scope to this cluster.
      start_time:   ISO 8601 start time.
      end_time:     ISO 8601 end time.
      service_name: Filter by service name.
      user_name:    Filter by user who performed the action.
      limit:        Maximum events to return (default 50).
    """
    if cluster_name:
        client = _pool.get_client_for_cluster(cluster_name)
        if client is None:
            return _no_client(cluster_name)
    else:
        # Pick first available client
        client = None
        for env_name in _pool.list_environments():
            client = _pool.get_client_for_environment(env_name)
            if client:
                break
        if client is None:
            return _dump({"error": "No CM clients available."})

    try:
        return _dump(
            await client.get_audit_events(
                cluster_name=cluster_name,
                start_time=start_time,
                end_time=end_time,
                service_name=service_name,
                user_name=user_name,
                limit=limit,
            )
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def list_datahubs() -> str:
    """
    List all DataHub clusters across all configured CM environments.
    Returns cluster name, type, version and environment.
    """
    results = []
    for env_name in _pool.list_environments():
        client = _pool.get_client_for_environment(env_name)
        if client is None:
            continue
        try:
            hubs = await client.list_datahubs()
            for hub in hubs:
                hub.setdefault("environment", env_name)
            results.extend(hubs)
        except Exception as exc:
            log.error("tool.list_datahubs.error", env=env_name, error=str(exc))
            results.append({"error": str(exc), "environment": env_name})
    return _dump(results)


# ── Registry management tools ─────────────────────────────────────────────────

@mcp.tool()
async def refresh_cluster_map() -> str:
    """
    Rebuild the cluster → CM mapping and re-discover service endpoints.
    Call this after adding a new cluster or after CM failover.
    """
    try:
        await _pool.refresh_cluster_map()
        return _dump(
            {
                "status": "ok",
                "clusters": _pool.list_known_clusters(),
                "environments": _pool.list_environments(),
            }
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def registry_list(include_inactive: bool = False) -> str:
    """
    List all registered CM instances (passwords excluded).

    Args:
      include_inactive: If True, include deactivated instances (default False).
    """
    try:
        return _dump(await _registry.async_list_raw(include_inactive=include_inactive))
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def registry_stats() -> str:
    """
    Return registry statistics: total instances, active/inactive count,
    breakdown by environment.
    """
    try:
        return _dump(await _registry.async_get_stats())
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def registry_add(
    host: str,
    port: int = 7183,
    username: str = "admin",
    password: str = "",
    environment_name: str = "default",
    use_tls: bool = True,
    verify_ssl: bool = True,
    api_version: str = "v51",
    timeout_seconds: int = 30,
) -> str:
    """
    Register a new Cloudera Manager instance.
    Not available with EnvRegistry (read-only backend).

    Args:
      host:             CM hostname or IP.
      port:             CM API port (default 7183).
      username:         CM username (default "admin").
      password:         CM password.
      environment_name: Logical environment label.
      use_tls:          Use HTTPS (default True).
      verify_ssl:       Verify TLS certificate (default True).
      api_version:      CM API version (default "v51").
      timeout_seconds:  Request timeout (default 30).
    """
    try:
        result = await _registry.async_register(
            host=host,
            port=port,
            username=username,
            password=password,
            environment_name=environment_name,
            use_tls=use_tls,
            verify_ssl=verify_ssl,
            api_version=api_version,
            timeout_seconds=timeout_seconds,
        )
        return _dump({"registered": result})
    except NotImplementedError as exc:
        return _dump({"error": str(exc)})
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def registry_deactivate(host: str) -> str:
    """
    Deactivate (soft-delete) a CM instance by hostname.
    The instance will no longer be used but remains in the registry.

    Args:
      host: CM hostname to deactivate.
    """
    try:
        await _registry.async_deactivate(host)
        return _dump({"deactivated": host})
    except NotImplementedError as exc:
        return _dump({"error": str(exc)})
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def registry_update_field(host: str, field: str, value: str) -> str:
    """
    Update a single field on a registered CM instance.
    Not available with EnvRegistry.

    Args:
      host:  CM hostname.
      field: Field name to update (e.g. "password", "port", "api_version").
      value: New value (always a string; will be coerced to the correct type).
    """
    try:
        await _registry.async_update_field(host, field, value)
        return _dump({"updated": host, "field": field})
    except NotImplementedError as exc:
        return _dump({"error": str(exc)})
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def registry_reload() -> str:
    """
    Reload registry from the backend (re-read YAML file or Iceberg table)
    and reconnect all CM clients.
    Use after manually editing cm_instances.yaml.
    """
    try:
        instances = await _registry.async_load()
        await _pool.reload(instances)
        return _dump(
            {
                "status": "ok",
                "instances": len(instances),
                "clusters": _pool.list_known_clusters(),
            }
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


# ── YARN tools ────────────────────────────────────────────────────────────────

@mcp.tool()
async def get_yarn_app(cluster_name: str, app_id: str) -> str:
    """
    Get the status and details of a YARN application by ID.
    Returns state, final_status, diagnostics (error message if failed),
    tracking_url, resource usage and timing information.
    Use this to diagnose why a Spark / MapReduce / Oozie job failed.

    Args:
      cluster_name: DataHub cluster name (use list_clusters to discover).
      app_id:       YARN application ID (e.g. application_1234567890_0001).
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.yarn_rm_url:
        return _dump(
            {
                "error": (
                    f"YARN ResourceManager endpoint not found for cluster '{cluster_name}'. "
                    "Ensure the YARN service is running and reachable."
                )
            }
        )
    client = YarnClient(endpoints.yarn_rm_url)
    try:
        return _dump(await client.get_app(app_id))
    except YarnNotFoundError as exc:
        return _dump({"error": str(exc)})
    except Exception as exc:
        return _dump({"error": f"YARN error: {exc}"})


@mcp.tool()
async def list_yarn_apps(
    cluster_name: str,
    state: Optional[str] = None,
    queue: Optional[str] = None,
    user: Optional[str] = None,
    limit: int = 20,
) -> str:
    """
    List recent YARN applications on a cluster.

    Args:
      cluster_name: Cluster name.
      state:        Filter by state (e.g. RUNNING, FINISHED, FAILED, KILLED).
      queue:        Filter by queue name.
      user:         Filter by submitting user.
      limit:        Maximum applications to return (default 20).
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.yarn_rm_url:
        return _dump(
            {
                "error": (
                    f"YARN ResourceManager endpoint not found for cluster '{cluster_name}'."
                )
            }
        )
    client = YarnClient(endpoints.yarn_rm_url)
    try:
        return _dump(
            await client.list_apps(state=state, queue=queue, user=user, limit=limit)
        )
    except Exception as exc:
        return _dump({"error": f"YARN error: {exc}"})


@mcp.tool()
async def get_yarn_queue(
    cluster_name: str,
    queue_name: Optional[str] = None,
) -> str:
    """
    Get YARN scheduler queue capacity and utilisation.
    If queue_name is omitted, returns the root queue summary.

    Args:
      cluster_name: Cluster name.
      queue_name:   Queue name to inspect (e.g. "default", "root.production").
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.yarn_rm_url:
        return _dump(
            {
                "error": (
                    f"YARN ResourceManager endpoint not found for cluster '{cluster_name}'."
                )
            }
        )
    client = YarnClient(endpoints.yarn_rm_url)
    try:
        return _dump(await client.get_queue(queue_name=queue_name))
    except Exception as exc:
        return _dump({"error": f"YARN error: {exc}"})


# ── Spark tools ───────────────────────────────────────────────────────────────

@mcp.tool()
async def get_spark_app(cluster_name: str, app_id: str) -> str:
    """
    Get Spark application details from the Spark History Server.
    Accepts both YARN application IDs and Spark application IDs.

    Args:
      cluster_name: Cluster name.
      app_id:       YARN application ID or Spark application ID.
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.spark_hs_url:
        return _dump(
            {
                "error": (
                    f"Spark History Server endpoint not found for cluster '{cluster_name}'. "
                    "Ensure the SPARK_ON_YARN service is running."
                )
            }
        )
    client = SparkClient(endpoints.spark_hs_url)
    try:
        return _dump(await client.get_app(app_id))
    except SparkNotFoundError as exc:
        return _dump({"error": str(exc)})
    except Exception as exc:
        return _dump({"error": f"Spark error: {exc}"})


@mcp.tool()
async def get_spark_stages(
    cluster_name: str,
    app_id: str,
    status: Optional[str] = None,
) -> str:
    """
    Get stage-level details for a Spark application.
    Useful to identify slow or failed stages.

    Args:
      cluster_name: Cluster name.
      app_id:       Spark or YARN application ID.
      status:       Filter by stage status (e.g. FAILED, ACTIVE, COMPLETE).
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.spark_hs_url:
        return _dump(
            {
                "error": (
                    f"Spark History Server endpoint not found for cluster '{cluster_name}'."
                )
            }
        )
    client = SparkClient(endpoints.spark_hs_url)
    try:
        return _dump(await client.get_stages(app_id, status=status))
    except Exception as exc:
        return _dump({"error": f"Spark error: {exc}"})


@mcp.tool()
async def list_spark_apps(
    cluster_name: str,
    status: Optional[str] = None,
    limit: int = 20,
) -> str:
    """
    List recent Spark applications from the Spark History Server.

    Args:
      cluster_name: Cluster name.
      status:       Filter by status (e.g. completed, running).
      limit:        Maximum applications to return (default 20).
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.spark_hs_url:
        return _dump(
            {
                "error": (
                    f"Spark History Server endpoint not found for cluster '{cluster_name}'."
                )
            }
        )
    client = SparkClient(endpoints.spark_hs_url)
    try:
        return _dump(await client.list_apps(status=status, limit=limit))
    except Exception as exc:
        return _dump({"error": f"Spark error: {exc}"})


# ── HDFS tools ────────────────────────────────────────────────────────────────

@mcp.tool()
async def get_namenode_status(cluster_name: str) -> str:
    """
    Get HDFS NameNode health status, capacity usage and block health.
    Returns health_summary (HEALTHY / DEGRADED / CRITICAL), under-replicated
    blocks, corrupt blocks, disk usage, and HA state.

    Args:
      cluster_name: Cluster name.
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.hdfs_nn_url:
        return _dump(
            {
                "error": (
                    f"HDFS NameNode endpoint not found for cluster '{cluster_name}'. "
                    "Ensure the HDFS service is running."
                )
            }
        )
    client = HdfsClient(endpoints.hdfs_nn_url)
    try:
        return _dump(await client.get_namenode_status())
    except Exception as exc:
        return _dump({"error": f"HDFS error: {exc}"})


# ── Oozie tools ───────────────────────────────────────────────────────────────

@mcp.tool()
async def get_oozie_job(cluster_name: str, job_id: str) -> str:
    """
    Get details of an Oozie workflow or coordinator job.
    For workflows, returns all action statuses including the YARN app_id of
    each action, which can be passed to get_yarn_app() for deeper diagnosis.

    Args:
      cluster_name: Cluster name.
      job_id:       Oozie job ID (e.g. 0000001-240101120000000-oozie-oozi-W).
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.oozie_url:
        return _dump(
            {
                "error": (
                    f"Oozie endpoint not found for cluster '{cluster_name}'. "
                    "Ensure the OOZIE service is running."
                )
            }
        )
    client = OozieClient(endpoints.oozie_url)
    try:
        return _dump(await client.get_job(job_id))
    except OozieNotFoundError as exc:
        return _dump({"error": str(exc)})
    except Exception as exc:
        return _dump({"error": f"Oozie error: {exc}"})


@mcp.tool()
async def list_oozie_jobs(
    cluster_name: str,
    status: Optional[str] = None,
    jobtype: str = "wf",
    user: Optional[str] = None,
    limit: int = 20,
) -> str:
    """
    List recent Oozie jobs.

    Args:
      cluster_name: Cluster name.
      status:       Filter by status (e.g. RUNNING, FAILED, SUCCEEDED, KILLED).
      jobtype:      Job type: "wf" (workflow) or "coordinator" (default "wf").
      user:         Filter by submitting user.
      limit:        Maximum jobs to return (default 20).
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.oozie_url:
        return _dump(
            {
                "error": (
                    f"Oozie endpoint not found for cluster '{cluster_name}'."
                )
            }
        )
    client = OozieClient(endpoints.oozie_url)
    try:
        return _dump(
            await client.list_jobs(
                status=status, jobtype=jobtype, user=user, limit=limit
            )
        )
    except Exception as exc:
        return _dump({"error": f"Oozie error: {exc}"})


# ── Entry point ───────────────────────────────────────────────────────────────

def run() -> None:
    """Entry point invoked by the cdp-mcp console script."""
    import structlog

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(__import__("logging"), server_cfg.log_level.upper(), 20)
        ),
    )
    mcp.run()


if __name__ == "__main__":
    run()
