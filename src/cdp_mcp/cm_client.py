"""
cm_client.py — Async HTTP client for the Cloudera Manager REST API.
(Original code by dvergari/cloudera-mcp-server, Apache 2.0)
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from cdp_mcp.config import ClouderaManagerSettings, ServerSettings

log = structlog.get_logger(__name__)


# ── Exception hierarchy ───────────────────────────────────────────────────────

class CMClientError(Exception):
    """Base exception for all CM client errors."""


class CMAuthError(CMClientError):
    """Authentication or authorisation failure."""


class CMNotFoundError(CMClientError):
    """Resource not found (HTTP 404)."""


class CMServiceUnavailable(CMClientError):
    """CM is temporarily unavailable (HTTP 503/504 or transport error)."""


class CMCommandFailed(CMClientError):
    """An asynchronous CM command finished with success=False."""


# ── Client ────────────────────────────────────────────────────────────────────

class ClouderaManagerClient:
    """Async HTTP client wrapping the Cloudera Manager REST API."""

    def __init__(
        self,
        settings: ClouderaManagerSettings,
        server_cfg: ServerSettings,
    ) -> None:
        self.cfg = settings
        self._server_cfg = server_cfg
        self._http: httpx.AsyncClient | None = None

    # ── Async context manager ─────────────────────────────────────────────────

    async def __aenter__(self) -> ClouderaManagerClient:
        await self.connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        self._http = httpx.AsyncClient(
            base_url=self.cfg.base_url,
            auth=(self.cfg.username, self.cfg.password),
            verify=self.cfg.effective_verify_ssl,
            timeout=httpx.Timeout(self.cfg.timeout_seconds),
            limits=httpx.Limits(
                max_connections=self._server_cfg.max_concurrent_requests,
                max_keepalive_connections=5,
            ),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )
        log.debug("cm_client.connected", host=self.cfg.effective_host)

    async def close(self) -> None:
        if self._http:
            await self._http.aclose()
            self._http = None
            log.debug("cm_client.closed", host=self.cfg.effective_host)

    # ── Retry / request helpers ───────────────────────────────────────────────

    def _retry_decorator(self):
        return retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=1, max=8),
            retry=retry_if_exception_type(
                (httpx.TransportError, CMServiceUnavailable)
            ),
            reraise=True,
        )

    async def _request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        json: Any | None = None,
        headers: dict | None = None,
    ) -> Any:
        assert self._http, "Client not initialised. Call connect() or use async with."

        @self._retry_decorator()
        async def _execute() -> Any:
            try:
                response = await self._http.request(
                    method, path, params=params, json=json, headers=headers
                )
            except httpx.TransportError as exc:
                log.warning("cm_client.transport_error", path=path, error=str(exc))
                raise

            if response.status_code == 401:
                raise CMAuthError(
                    f"Authentication failed for {self.cfg.host}. "
                    "Check username and password."
                )
            if response.status_code == 403:
                raise CMAuthError(f"Permission denied: {path}")
            if response.status_code == 404:
                raise CMNotFoundError(f"Resource not found: {path}")
            if response.status_code in (503, 504):
                log.warning(
                    "cm_client.server_unavailable",
                    status=response.status_code,
                    path=path,
                )
                raise CMServiceUnavailable(
                    f"CM unavailable (HTTP {response.status_code}): {self.cfg.host}"
                )
            if response.status_code >= 400:
                raise CMClientError(
                    f"CM returned HTTP {response.status_code}: "
                    f"{response.text[:300]}"
                )
            return response.json() if response.content else {}

        return await _execute()

    async def _get(self, path: str, params: dict | None = None) -> Any:
        log.debug("cm_client.get", path=path)
        return await self._request("GET", path, params=params)

    async def _put(self, path: str, json: Any = None) -> Any:
        log.debug("cm_client.put", path=path)
        return await self._request("PUT", path, json=json)

    async def _post(self, path: str, json: Any = None) -> Any:
        log.debug("cm_client.post", path=path)
        return await self._request("POST", path, json=json)

    async def _get_text(self, path: str, params: dict | None = None) -> str:
        assert self._http, "Client not initialised."
        log.debug("cm_client.get_text", path=path)
        response = await self._http.get(path, params=params)
        if response.status_code == 404:
            raise CMNotFoundError(f"Resource not found: {path}")
        if response.status_code >= 400:
            raise CMClientError(f"CM returned HTTP {response.status_code}")
        return response.text

    # ── Utility ───────────────────────────────────────────────────────────────

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).isoformat()

    @staticmethod
    def _validate_time_range(
        start_time: str | None,
        end_time: str | None,
    ) -> tuple[str, str]:
        from datetime import timedelta
        now = datetime.now(UTC)
        if end_time is None:
            end_time = now.isoformat()
        if start_time is None:
            start_time = (now - timedelta(hours=1)).isoformat()
        return start_time, end_time

    # ── Cluster / service ─────────────────────────────────────────────────────

    async def list_clusters(self) -> list[dict]:
        data = await self._get("/clusters")
        return data.get("items", [])

    async def list_services(self, cluster_name: str) -> list[dict]:
        data = await self._get(f"/clusters/{cluster_name}/services")
        return data.get("items", [])

    async def get_service(self, cluster_name: str, service_name: str) -> dict:
        return await self._get(f"/clusters/{cluster_name}/services/{service_name}")

    # ── Logs ──────────────────────────────────────────────────────────────────

    async def get_service_logs(
        self,
        cluster_name: str,
        service_name: str,
        max_lines: int = 500,
    ) -> dict[str, list[str]]:
        """
        Retrieve logs for all roles of a service in parallel (max 5 concurrent).
        Returns a dict of {role_name: [log_lines]}.
        """
        roles_data = await self._get(
            f"/clusters/{cluster_name}/services/{service_name}/roles"
        )
        roles = roles_data.get("items", [])
        log.info(
            "cm_client.get_service_logs",
            cluster=cluster_name,
            service=service_name,
            num_roles=len(roles),
        )

        semaphore = asyncio.Semaphore(5)
        result: dict[str, list[str]] = {}

        async def _fetch_role_log(role: dict) -> None:
            role_name = role.get("name", "unknown")
            async with semaphore:
                try:
                    text = await self._get_text(
                        f"/clusters/{cluster_name}/services/{service_name}"
                        f"/roles/{role_name}/logs/full",
                        params={"lines": max_lines},
                    )
                    result[role_name] = text.splitlines()
                except CMNotFoundError:
                    log.debug(
                        "cm_client.role_log_not_found",
                        role=role_name,
                    )
                    result[role_name] = []
                except Exception as exc:
                    log.warning(
                        "cm_client.role_log_error",
                        role=role_name,
                        error=str(exc),
                    )
                    result[role_name] = [f"[Error fetching log: {exc}]"]

        await asyncio.gather(*[_fetch_role_log(r) for r in roles])
        return result

    # ── Alerts / events ───────────────────────────────────────────────────────

    async def get_alerts(
        self,
        cluster_name: str,
        category: str | None = None,
        severity: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        start_time, end_time = self._validate_time_range(start_time, end_time)
        params: dict[str, Any] = {
            "from": start_time,
            "to": end_time,
            "maxResults": limit,
        }
        if category:
            params["category"] = category
        if severity:
            params["severity"] = severity
        data = await self._get(f"/clusters/{cluster_name}/events", params=params)
        return data.get("items", [])

    # ── Metrics ───────────────────────────────────────────────────────────────

    async def get_service_metrics(
        self,
        cluster_name: str,
        service_name: str,
        metric_names: list[str],
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[dict]:
        start_time, end_time = self._validate_time_range(start_time, end_time)
        metric_selector = ", ".join(metric_names)
        tsquery = (
            f"SELECT {metric_selector} "
            f"WHERE clusterName = {cluster_name!r} "
            f"AND serviceName = {service_name!r}"
        )
        body = {
            "query": tsquery,
            "from": start_time,
            "to": end_time,
        }
        data = await self._post("/timeseries", json=body)
        return data.get("items", [])

    # ── Config ────────────────────────────────────────────────────────────────

    async def get_config(
        self,
        cluster_name: str,
        service_name: str,
        view: str = "full",
    ) -> list[dict]:
        data = await self._get(
            f"/clusters/{cluster_name}/services/{service_name}/config",
            params={"view": view},
        )
        return data.get("items", [])

    async def update_config(
        self,
        cluster_name: str,
        service_name: str,
        configs: list[dict],
    ) -> dict:
        """
        Update service config items.
        configs: list of {"name": str, "value": str} dicts.
        """
        body = {"items": configs}
        return await self._put(
            f"/clusters/{cluster_name}/services/{service_name}/config",
            json=body,
        )

    # ── Commands ──────────────────────────────────────────────────────────────

    async def run_service_command(
        self,
        cluster_name: str,
        service_name: str,
        command: str,
    ) -> dict:
        data = await self._post(
            f"/clusters/{cluster_name}/services/{service_name}/commands/{command}"
        )
        return {
            "id": data.get("id"),
            "name": data.get("name"),
            "active": data.get("active"),
            "success": data.get("success"),
            "resultMessage": data.get("resultMessage"),
        }

    async def get_command_status(self, command_id: int) -> dict:
        data = await self._get(f"/commands/{command_id}")
        return {
            "id": data.get("id"),
            "name": data.get("name"),
            "active": data.get("active"),
            "success": data.get("success"),
            "resultMessage": data.get("resultMessage"),
        }

    # ── Hosts ─────────────────────────────────────────────────────────────────

    async def get_host_status(
        self,
        cluster_name: str | None = None,
        host_filter: str | None = None,
    ) -> list[dict]:
        if cluster_name:
            data = await self._get(f"/clusters/{cluster_name}/hosts")
        else:
            params: dict[str, Any] = {}
            if host_filter:
                params["filter"] = host_filter
            data = await self._get("/hosts", params=params or None)

        hosts = data.get("items", [])
        result = []
        for host in hosts:
            result.append(
                {
                    "hostname": host.get("hostname"),
                    "hostId": host.get("hostId"),
                    "ipAddress": host.get("ipAddress"),
                    "healthSummary": host.get("healthSummary"),
                    "entityStatus": host.get("entityStatus"),
                    "numCores": host.get("numCores"),
                    "totalPhysMemBytes": host.get("totalPhysMemBytes"),
                    "roleRefs": host.get("roleRefs", []),
                }
            )
        return result

    # ── Audit events ──────────────────────────────────────────────────────────

    async def get_audit_events(
        self,
        cluster_name: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        service_name: str | None = None,
        user_name: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        start_time, end_time = self._validate_time_range(start_time, end_time)
        params: dict[str, Any] = {
            "startTime": start_time,
            "endTime": end_time,
            "maxResults": limit,
        }
        if service_name:
            params["service"] = service_name
        if user_name:
            params["user"] = user_name
        if cluster_name:
            data = await self._get(
                f"/clusters/{cluster_name}/audits", params=params
            )
        else:
            data = await self._get("/audits", params=params)
        return data.get("items", [])

    # ── Service / role management ─────────────────────────────────────────────

    async def delete_service(self, cluster_name: str, service_name: str) -> dict:
        """Delete a service from a cluster. Returns the deleted service object."""
        return await self._request(
            "DELETE",
            f"/clusters/{cluster_name}/services/{service_name}",
        )

    # ── Role management ───────────────────────────────────────────────────────

    async def delete_role(
        self,
        cluster_name: str,
        service_name: str,
        role_name: str,
    ) -> dict:
        """Delete a role instance from a service. Returns the deleted role object."""
        return await self._request(
            "DELETE",
            f"/clusters/{cluster_name}/services/{service_name}/roles/{role_name}",
        )

    # ── Management Service ────────────────────────────────────────────────────

    async def get_mgmt_service(self) -> dict:
        """Return CM Management Service state and health summary."""
        return await self._get("/cm/service")

    async def get_mgmt_service_roles(self) -> list[dict]:
        """Return all role instances of the CM Management Service."""
        data = await self._get("/cm/service/roles")
        return data.get("items", [])

    # ── DataHubs ──────────────────────────────────────────────────────────────

    async def list_datahubs(self) -> list[dict]:
        """
        List available DataHub clusters.
        For Knox proxy environments this returns the configured cluster name.
        For direct CM connections this is equivalent to list_clusters().
        """
        if self.cfg.use_knox and self.cfg.cluster_name:
            return [
                {
                    "name": self.cfg.cluster_name,
                    "displayName": self.cfg.cluster_name,
                    "fullVersion": "unknown",
                    "clusterType": "DATAHUB",
                    "environment": self.cfg.environment_name,
                }
            ]
        return await self.list_clusters()
