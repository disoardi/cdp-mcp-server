"""
iceberg.py — IcebergRegistry: stores CM instances in an Iceberg table via Impala.
(Original CMRegistry code by dvergari/cloudera-mcp-server, Apache 2.0)
"""
from __future__ import annotations

import threading
from typing import Optional

import structlog

from cdp_mcp.config import ClouderaManagerSettings, ImpalaSettings
from cdp_mcp.registry.base import BaseRegistry

log = structlog.get_logger(__name__)


# ── Exceptions ────────────────────────────────────────────────────────────────

class RegistryError(Exception):
    """Base exception for registry operations."""


class RegistryConnectionError(RegistryError):
    """Failed to connect to Impala / HiveServer2."""


class RegistryNotFoundError(RegistryError):
    """Requested CM instance not found in registry."""


class RegistryDuplicateError(RegistryError):
    """Attempted to register a CM instance that already exists."""


# ── IcebergRegistry ───────────────────────────────────────────────────────────

class IcebergRegistry(BaseRegistry):
    """
    Registry backend backed by an Apache Iceberg table (read/written via Impala).

    Table schema (DDL):
        CREATE TABLE IF NOT EXISTS <db>.<table> (
            host             STRING,
            port             INT,
            username         STRING,
            password         STRING,
            use_tls          BOOLEAN,
            verify_ssl       BOOLEAN,
            api_version      STRING,
            timeout_seconds  INT,
            environment_name STRING,
            active           BOOLEAN,
            use_knox         BOOLEAN,
            lb_host          STRING,
            lb_port          INT,
            cluster_name     STRING
        )
        STORED AS ICEBERG;
    """

    _CREATE_TABLE_SQL = """
        CREATE TABLE IF NOT EXISTS {db}.{table} (
            host             STRING,
            port             INT,
            username         STRING,
            password         STRING,
            use_tls          BOOLEAN,
            verify_ssl       BOOLEAN,
            api_version      STRING,
            timeout_seconds  INT,
            environment_name STRING,
            active           BOOLEAN,
            use_knox         BOOLEAN,
            lb_host          STRING,
            lb_port          INT,
            cluster_name     STRING
        )
        STORED AS ICEBERG
    """

    def __init__(self, impala_cfg: ImpalaSettings) -> None:
        self._cfg = impala_cfg
        self._conn = None
        self._lock = threading.RLock()
        self._cache: list[ClouderaManagerSettings] = []

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        log.info(
            "iceberg_registry.start",
            host=self._cfg.host,
            db=self._cfg.database,
            table=self._cfg.table,
        )
        self._connect()
        self._ensure_table()
        self.load()

    def stop(self) -> None:
        with self._lock:
            if self._conn:
                try:
                    self._conn.close()
                except Exception as exc:
                    log.warning("iceberg_registry.close_error", error=str(exc))
                self._conn = None
        log.info("iceberg_registry.stop")

    def _connect(self) -> None:
        try:
            from impala.dbapi import connect  # type: ignore[import]
        except ImportError as exc:
            raise RegistryConnectionError(
                "impyla is not installed. Run: pip install impyla thrift thrift-sasl"
            ) from exc

        kwargs: dict = {
            "host": self._cfg.host,
            "port": self._cfg.port,
            "user": self._cfg.username,
            "password": self._cfg.password,
            "auth_mechanism": self._cfg.auth_mechanism,
            "use_ssl": self._cfg.use_ssl,
            "timeout": self._cfg.timeout_seconds,
        }
        if self._cfg.use_ssl:
            kwargs["ca_cert"] = None  # use system CAs
        try:
            self._conn = connect(**kwargs)
            log.info("iceberg_registry.connected", host=self._cfg.host)
        except Exception as exc:
            raise RegistryConnectionError(
                f"Failed to connect to Impala at {self._cfg.host}:{self._cfg.port}: {exc}"
            ) from exc

    def _cursor(self):
        if self._conn is None:
            self._connect()
        return self._conn.cursor()

    def _ensure_table(self) -> None:
        sql = self._CREATE_TABLE_SQL.format(
            db=self._cfg.database, table=self._cfg.table
        )
        with self._lock:
            cur = self._cursor()
            try:
                cur.execute(sql)
                log.debug("iceberg_registry.table_ensured", table=self._cfg.table)
            except Exception as exc:
                log.warning("iceberg_registry.ensure_table_error", error=str(exc))
            finally:
                cur.close()

    # ── Read operations ───────────────────────────────────────────────────────

    def load(self) -> list[ClouderaManagerSettings]:
        sql = (
            f"SELECT host, port, username, password, use_tls, verify_ssl, "
            f"api_version, timeout_seconds, environment_name, active, "
            f"use_knox, lb_host, lb_port, cluster_name "
            f"FROM {self._cfg.database}.{self._cfg.table} "
            f"WHERE active = TRUE"
        )
        with self._lock:
            cur = self._cursor()
            try:
                cur.execute(sql)
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
            finally:
                cur.close()

            result = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                try:
                    settings = ClouderaManagerSettings(**row_dict)
                    result.append(settings)
                except Exception as exc:
                    log.error(
                        "iceberg_registry.invalid_row",
                        host=row_dict.get("host"),
                        error=str(exc),
                    )
            self._cache = result
            log.info("iceberg_registry.loaded", count=len(result))
            return result

    def get_all(self) -> list[ClouderaManagerSettings]:
        with self._lock:
            return list(self._cache)

    def list_raw(self, include_inactive: bool = False) -> list[dict]:
        where = "" if include_inactive else "WHERE active = TRUE"
        sql = (
            f"SELECT host, port, username, use_tls, verify_ssl, api_version, "
            f"timeout_seconds, environment_name, active, use_knox, lb_host, "
            f"lb_port, cluster_name "
            f"FROM {self._cfg.database}.{self._cfg.table} {where}"
        )
        with self._lock:
            cur = self._cursor()
            try:
                cur.execute(sql)
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
            finally:
                cur.close()
            return [dict(zip(columns, row)) for row in rows]

    def get_stats(self) -> dict:
        sql_total = (
            f"SELECT active, COUNT(*) AS cnt "
            f"FROM {self._cfg.database}.{self._cfg.table} "
            f"GROUP BY active"
        )
        sql_env = (
            f"SELECT environment_name, COUNT(*) AS cnt "
            f"FROM {self._cfg.database}.{self._cfg.table} "
            f"WHERE active = TRUE "
            f"GROUP BY environment_name"
        )
        with self._lock:
            cur = self._cursor()
            try:
                cur.execute(sql_total)
                totals = dict(cur.fetchall())
                cur.execute(sql_env)
                by_env = dict(cur.fetchall())
            finally:
                cur.close()

        active = totals.get(True, totals.get("true", 0))
        inactive = totals.get(False, totals.get("false", 0))
        return {
            "backend": "iceberg",
            "table": f"{self._cfg.database}.{self._cfg.table}",
            "total": active + inactive,
            "active": active,
            "inactive": inactive,
            "by_environment": by_env,
        }

    # ── Write operations ──────────────────────────────────────────────────────

    def register(self, **kwargs) -> str:
        host = kwargs.get("host", "")
        if not host:
            raise ValueError("'host' is required.")

        # Check for duplicates
        check_sql = (
            f"SELECT COUNT(*) FROM {self._cfg.database}.{self._cfg.table} "
            f"WHERE host = '{host}'"
        )
        with self._lock:
            cur = self._cursor()
            try:
                cur.execute(check_sql)
                count = cur.fetchone()[0]
            finally:
                cur.close()

        if count > 0:
            raise RegistryDuplicateError(f"Host '{host}' already registered.")

        defaults = {
            "port": 7183,
            "username": "admin",
            "password": "",
            "use_tls": True,
            "verify_ssl": True,
            "api_version": "v51",
            "timeout_seconds": 30,
            "environment_name": "default",
            "active": True,
            "use_knox": False,
            "lb_host": None,
            "lb_port": 443,
            "cluster_name": None,
        }
        defaults.update(kwargs)

        sql = (
            f"INSERT INTO {self._cfg.database}.{self._cfg.table} "
            f"(host, port, username, password, use_tls, verify_ssl, api_version, "
            f"timeout_seconds, environment_name, active, use_knox, lb_host, lb_port, cluster_name) "
            f"VALUES ("
            f"'{defaults['host']}', {defaults['port']}, '{defaults['username']}', "
            f"'{defaults['password']}', {defaults['use_tls']}, {defaults['verify_ssl']}, "
            f"'{defaults['api_version']}', {defaults['timeout_seconds']}, "
            f"'{defaults['environment_name']}', {defaults['active']}, "
            f"{defaults['use_knox']}, "
            f"{'NULL' if defaults['lb_host'] is None else repr(defaults['lb_host'])}, "
            f"{defaults['lb_port']}, "
            f"{'NULL' if defaults['cluster_name'] is None else repr(defaults['cluster_name'])}"
            f")"
        )
        with self._lock:
            cur = self._cursor()
            try:
                cur.execute(sql)
            finally:
                cur.close()
        self.load()
        log.info("iceberg_registry.registered", host=host)
        return host

    def deactivate(self, host: str) -> None:
        sql = (
            f"UPDATE {self._cfg.database}.{self._cfg.table} "
            f"SET active = FALSE WHERE host = '{host}'"
        )
        with self._lock:
            cur = self._cursor()
            try:
                cur.execute(sql)
                if cur.rowcount == 0:
                    raise RegistryNotFoundError(f"Host '{host}' not found.")
            finally:
                cur.close()
        self.load()
        log.info("iceberg_registry.deactivated", host=host)

    def update_field(self, host: str, field: str, value: str) -> None:
        READONLY_FIELDS = {"host"}
        ALLOWED_FIELDS = {
            "port", "username", "password", "use_tls", "verify_ssl",
            "api_version", "timeout_seconds", "environment_name",
            "active", "use_knox", "lb_host", "lb_port", "cluster_name",
        }
        if field in READONLY_FIELDS:
            raise ValueError(f"Field '{field}' is read-only.")
        if field not in ALLOWED_FIELDS:
            raise ValueError(f"Unknown field '{field}'.")

        sql = (
            f"UPDATE {self._cfg.database}.{self._cfg.table} "
            f"SET {field} = '{value}' WHERE host = '{host}'"
        )
        with self._lock:
            cur = self._cursor()
            try:
                cur.execute(sql)
                if cur.rowcount == 0:
                    raise RegistryNotFoundError(f"Host '{host}' not found.")
            finally:
                cur.close()
        self.load()
        log.info("iceberg_registry.updated", host=host, field=field)
