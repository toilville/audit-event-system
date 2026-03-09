"""
forge_config.py — read infrastructure config from ~/.spel/forge-manifest.spel

Priority: env var → manifest → RuntimeError (no hardcoded defaults)

The manifest is the single source of truth for all infrastructure endpoints.
"""

import os
from pathlib import Path
from typing import Optional

MANIFEST_PATH = Path.home() / ".spel" / "forge-manifest.spel"

_cache: Optional[dict] = None


def _parse_manifest() -> dict:
    """Extract key/value pairs from forge-manifest.spel using indented section tracking."""
    if not MANIFEST_PATH.exists():
        return {}

    result: dict = {}
    section_stack: list[tuple[int, str]] = []  # (indent, key)

    for raw in MANIFEST_PATH.read_text().splitlines():
        stripped = raw.rstrip()
        if not stripped or stripped.lstrip().startswith("#"):
            continue

        indent = len(stripped) - len(stripped.lstrip())
        content = stripped.lstrip()

        # Pop sections that are at the same or deeper indent level
        while section_stack and section_stack[-1][0] >= indent:
            section_stack.pop()

        if ":" not in content:
            continue

        key, _, value = content.partition(":")
        key = key.strip()
        value = value.strip()

        dot_path = ".".join(s for _, s in section_stack) + ("." if section_stack else "") + key

        if value and not value.startswith("#"):
            result[dot_path] = value
        else:
            section_stack.append((indent, key))

    return result


def _manifest() -> dict:
    global _cache
    if _cache is None:
        _cache = _parse_manifest()
    return _cache


def kafka_broker() -> str:
    """Return the canonical Kafka broker address. KAFKA_BROKERS env overrides."""
    if env := os.getenv("KAFKA_BROKERS"):
        return env
    if broker := _manifest().get("infrastructure.kafka.broker"):
        return broker
    raise RuntimeError(
        f"Kafka broker not configured. Set KAFKA_BROKERS or add "
        f"infrastructure.kafka.broker to {MANIFEST_PATH}"
    )


def database_url() -> str:
    """Return the canonical database URL. DATABASE_URL env overrides."""
    if env := os.getenv("DATABASE_URL"):
        return env
    if url := _manifest().get("infrastructure.database.url"):
        return url
    raise RuntimeError(
        f"Database URL not configured. Set DATABASE_URL or add "
        f"infrastructure.database.url to {MANIFEST_PATH}"
    )


def tenant_id() -> str:
    """Return the forge tenant ID. TENANT_ID env overrides."""
    if env := os.getenv("TENANT_ID"):
        return env
    return _manifest().get("forge.tenant", "spel-local-forge")
