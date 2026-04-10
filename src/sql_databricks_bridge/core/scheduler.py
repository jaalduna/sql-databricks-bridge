"""Unified cron-like scheduler that reads schedules.yaml and runs tasks on time."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import yaml

from sql_databricks_bridge.core.paths import get_config_file

logger = logging.getLogger(__name__)


@dataclass
class ScheduleEntry:
    """Parsed schedule for a single task."""

    name: str
    enabled: bool = False
    hours: list[int] = field(default_factory=lambda: [4])
    minutes: list[int] = field(default_factory=lambda: [0])
    weekdays_only: bool = True
    extra: dict[str, Any] = field(default_factory=dict)


def load_schedules(override_path: str = "") -> dict[str, ScheduleEntry]:
    """Load schedules from YAML (bundled default or override path).

    Args:
        override_path: If non-empty, read from this path instead of bundled.

    Returns:
        Dict mapping task name to ScheduleEntry.
    """
    if override_path and Path(override_path).is_file():
        raw = Path(override_path).read_text(encoding="utf-8")
    else:
        config_path = get_config_file("schedules.yaml")
        raw = config_path.read_text(encoding="utf-8")

    data = yaml.safe_load(raw) or {}
    schedules_data = data.get("schedules", {})

    entries: dict[str, ScheduleEntry] = {}
    for name, cfg in schedules_data.items():
        if not isinstance(cfg, dict):
            continue
        known_keys = {"enabled", "hours", "minutes", "minute", "weekdays_only"}
        extra = {k: v for k, v in cfg.items() if k not in known_keys}

        # Support both "minutes: [0, 15, 30]" (list) and legacy "minute: 0" (int)
        raw_minutes = cfg.get("minutes", cfg.get("minute", 0))
        if isinstance(raw_minutes, int):
            raw_minutes = [raw_minutes]

        entries[name] = ScheduleEntry(
            name=name,
            enabled=cfg.get("enabled", False),
            hours=cfg.get("hours", [4]),
            minutes=raw_minutes,
            weekdays_only=cfg.get("weekdays_only", True),
            extra=extra,
        )

    return entries


class TaskScheduler:
    """Runs registered async tasks based on schedules.yaml timing.

    Usage:
        scheduler = TaskScheduler()
        scheduler.register("event_poller", my_async_func)
        task = asyncio.create_task(scheduler.start())
    """

    def __init__(self, schedules_path: str = "") -> None:
        self._schedules = load_schedules(schedules_path)
        self._handlers: dict[str, Callable[..., Awaitable[None]]] = {}
        self._last_run: dict[str, str] = {}  # "name" -> "YYYY-MM-DD-HH-MM"
        self._running = False

    @property
    def schedules(self) -> dict[str, ScheduleEntry]:
        return self._schedules

    def reload(self, schedules_path: str = "") -> None:
        """Hot-reload schedules from YAML. Takes effect on the next 30s tick."""
        self._schedules = load_schedules(schedules_path)
        logger.info("Schedules reloaded")
        self._log_enabled()

    def register(self, name: str, handler: Callable[..., Awaitable[None]]) -> None:
        """Register an async handler for a named schedule."""
        self._handlers[name] = handler

    async def start(self) -> None:
        """Main loop — checks every 30s if any task should run.

        Re-reads self._schedules each iteration so hot-reload via
        reload() takes effect without restarting the server.
        """
        self._running = True
        self._log_enabled()

        while self._running:
            try:
                now = datetime.now(timezone.utc)
                for name, entry in self._schedules.items():
                    if entry.enabled and name in self._handlers:
                        self._check_and_run(name, entry, now)
            except Exception as e:
                logger.error("Scheduler loop error: %s", e)

            await asyncio.sleep(30)

    def _log_enabled(self) -> None:
        """Log which schedules are currently enabled."""
        enabled = [
            name for name, entry in self._schedules.items()
            if entry.enabled and name in self._handlers
        ]
        if enabled:
            for name in enabled:
                entry = self._schedules[name]
                mins_str = ",".join(f":{m:02d}" for m in entry.minutes)
                logger.info(
                    "Scheduler: %s enabled — hours=%s, minutes=%s, weekdays_only=%s",
                    name, entry.hours, mins_str, entry.weekdays_only,
                )
        else:
            logger.info("Scheduler: no enabled schedules with registered handlers")

    def stop(self) -> None:
        self._running = False
        logger.info("Scheduler stopped")

    def _check_and_run(self, name: str, entry: ScheduleEntry, now: datetime) -> None:
        run_key = f"{now.date()}-{now.hour:02d}-{now.minute:02d}"

        should_run = (
            now.hour in entry.hours
            and now.minute in entry.minutes
            and run_key != self._last_run.get(name)
            and (not entry.weekdays_only or now.weekday() < 5)
        )

        if should_run:
            self._last_run[name] = run_key
            handler = self._handlers[name]
            logger.info("Scheduler: triggering %s at %s UTC", name, now.strftime("%H:%M"))
            asyncio.create_task(self._run_handler(name, handler, entry))

    async def _run_handler(
        self, name: str, handler: Callable[..., Awaitable[None]], entry: ScheduleEntry
    ) -> None:
        try:
            await handler(entry)
            logger.info("Scheduler: %s completed", name)
        except Exception as e:
            logger.error("Scheduler: %s failed: %s", name, e)
