"""
file_registry.py — FileRegistry: reads CM instances from a YAML file.

Schema:
  instances:
    - host: cm.example.com
      port: 7183
      username: admin
      password: "${CM_PASSWORD}"
      environment_name: dev
      use_tls: false
      verify_ssl: false
      api_version: v51
      timeout_seconds: 30
      # optional:
      use_knox: false
      # endpoints_override:
      #   yarn_rm: http://rm:8088
"""
from __future__ import annotations

import os
import re
import threading
from pathlib import Path
from typing import Optional

import structlog
import yaml

from cdp_mcp.config import ClouderaManagerSettings
from cdp_mcp.registry.base import BaseRegistry

log = structlog.get_logger(__name__)

_ENV_VAR_RE = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}")


def _interpolate(value: str) -> str:
    """Replace ${VAR_NAME} with the corresponding environment variable."""
    def _replace(m: re.Match) -> str:
        var = m.group(1)
        resolved = os.environ.get(var, "")
        if not resolved:
            log.warning("file_registry.env_var_not_set", var=var)
        return resolved
    return _ENV_VAR_RE.sub(_replace, value)


def _interpolate_dict(d: dict) -> dict:
    """Recursively interpolate env vars in all string values of a dict."""
    result = {}
    for k, v in d.items():
        if isinstance(v, str):
            result[k] = _interpolate(v)
        elif isinstance(v, dict):
            result[k] = _interpolate_dict(v)
        else:
            result[k] = v
    return result


class FileRegistry(BaseRegistry):
    """
    Registry backend backed by a YAML file.

    Thread-safe via RLock. Supports env var interpolation in values.
    Read-write: register/deactivate/update_field modify the YAML file.
    """

    def __init__(self, file_path: str = "cm_instances.yaml") -> None:
        self._path = Path(file_path)
        self._lock = threading.RLock()
        self._cache: list[ClouderaManagerSettings] = []
        self._raw_data: list[dict] = []

    def start(self) -> None:
        log.info("file_registry.start", path=str(self._path))
        self.load()

    def stop(self) -> None:
        log.info("file_registry.stop")

    def _read_file(self) -> list[dict]:
        if not self._path.exists():
            log.warning("file_registry.file_not_found", path=str(self._path))
            return []
        with self._path.open("r") as f:
            data = yaml.safe_load(f) or {}
        return data.get("instances", [])

    def _write_file(self, instances: list[dict]) -> None:
        with self._path.open("w") as f:
            yaml.safe_dump(
                {"instances": instances},
                f,
                default_flow_style=False,
                allow_unicode=True,
            )

    def load(self) -> list[ClouderaManagerSettings]:
        with self._lock:
            raw_instances = self._read_file()
            self._raw_data = raw_instances
            result = []
            for raw in raw_instances:
                interpolated = _interpolate_dict(raw)
                active = interpolated.pop("active", True)
                if not active:
                    continue
                # Remove endpoints_override before building settings dataclass
                endpoints_override = interpolated.pop("endpoints_override", None)
                try:
                    settings = ClouderaManagerSettings(**interpolated)
                    if endpoints_override:
                        object.__setattr__(settings, "endpoints_override", endpoints_override)
                    result.append(settings)
                except Exception as exc:
                    log.error(
                        "file_registry.invalid_instance",
                        error=str(exc),
                        raw=raw,
                    )
            self._cache = result
            log.info(
                "file_registry.loaded",
                count=len(result),
                path=str(self._path),
            )
            return result

    def get_all(self) -> list[ClouderaManagerSettings]:
        with self._lock:
            return list(self._cache)

    def list_raw(self, include_inactive: bool = False) -> list[dict]:
        with self._lock:
            raw = self._raw_data if self._raw_data else self._read_file()
            result = []
            for inst in raw:
                if not include_inactive and not inst.get("active", True):
                    continue
                safe = {k: v for k, v in inst.items() if k != "password"}
                result.append(safe)
            return result

    def get_stats(self) -> dict:
        with self._lock:
            all_raw = self._read_file()
            active = [i for i in all_raw if i.get("active", True)]
            inactive = [i for i in all_raw if not i.get("active", True)]
            by_env: dict[str, int] = {}
            for inst in active:
                env = inst.get("environment_name", "default")
                by_env[env] = by_env.get(env, 0) + 1
            return {
                "backend": "file",
                "file_path": str(self._path),
                "total": len(all_raw),
                "active": len(active),
                "inactive": len(inactive),
                "by_environment": by_env,
            }

    def register(self, **kwargs) -> str:
        with self._lock:
            instances = self._read_file()
            host = kwargs.get("host", "")
            for inst in instances:
                if inst.get("host") == host:
                    raise ValueError(f"Host '{host}' already registered.")
            instances.append(kwargs)
            self._write_file(instances)
            self._raw_data = instances
            self.load()
            log.info("file_registry.registered", host=host)
            return host

    def deactivate(self, host: str) -> None:
        with self._lock:
            instances = self._read_file()
            found = False
            for inst in instances:
                if inst.get("host") == host:
                    inst["active"] = False
                    found = True
                    break
            if not found:
                raise ValueError(f"Host '{host}' not found in registry.")
            self._write_file(instances)
            self._raw_data = instances
            self.load()
            log.info("file_registry.deactivated", host=host)

    def update_field(self, host: str, field: str, value: str) -> None:
        READONLY_FIELDS = {"host"}
        if field in READONLY_FIELDS:
            raise ValueError(f"Field '{field}' is read-only.")
        with self._lock:
            instances = self._read_file()
            found = False
            for inst in instances:
                if inst.get("host") == host:
                    inst[field] = value
                    found = True
                    break
            if not found:
                raise ValueError(f"Host '{host}' not found in registry.")
            self._write_file(instances)
            self._raw_data = instances
            self.load()
            log.info("file_registry.updated", host=host, field=field)
