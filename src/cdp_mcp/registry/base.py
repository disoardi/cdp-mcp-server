"""
base.py — Abstract base class for all CM registry backends.
"""
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod

from cdp_mcp.config import ClouderaManagerSettings


class BaseRegistry(ABC):
    """
    Abstract base class that all registry backends must implement.

    Backends: IcebergRegistry (Iceberg/Impala), FileRegistry (YAML), EnvRegistry (env vars).
    """

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    @abstractmethod
    def start(self) -> None:
        """Initialize the registry (open connections, load data, start background threads)."""

    @abstractmethod
    def stop(self) -> None:
        """Shut down the registry (close connections, stop background threads)."""

    # ── Read operations ───────────────────────────────────────────────────────

    @abstractmethod
    def get_all(self) -> list[ClouderaManagerSettings]:
        """Return all active ClouderaManagerSettings instances."""

    @abstractmethod
    def load(self) -> list[ClouderaManagerSettings]:
        """Force-reload from the backend and return all active instances."""

    @abstractmethod
    def list_raw(self, include_inactive: bool = False) -> list[dict]:
        """Return raw dicts (passwords excluded) for all instances."""

    @abstractmethod
    def get_stats(self) -> dict:
        """Return registry statistics (total, active, by environment, etc.)."""

    # ── Write operations ──────────────────────────────────────────────────────

    @abstractmethod
    def register(self, **kwargs) -> str:
        """Register a new CM instance. Returns the host identifier."""

    @abstractmethod
    def deactivate(self, host: str) -> None:
        """Mark a CM instance as inactive (soft delete)."""

    @abstractmethod
    def update_field(self, host: str, field: str, value: str) -> None:
        """Update a single field on a CM instance."""

    # ── Async wrappers ────────────────────────────────────────────────────────

    async def async_load(self) -> list[ClouderaManagerSettings]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.load)

    async def async_list_raw(self, include_inactive: bool = False) -> list[dict]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.list_raw, include_inactive)

    async def async_register(self, **kwargs) -> str:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: self.register(**kwargs))

    async def async_deactivate(self, host: str) -> None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.deactivate, host)

    async def async_update_field(self, host: str, field: str, value: str) -> None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.update_field, host, field, value)

    async def async_get_stats(self) -> dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.get_stats)
