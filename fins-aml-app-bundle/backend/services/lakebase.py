"""Lakebase Postgres helper for the graph queries.

Provides an `execute_query` coroutine matching DatabaseService's interface,
so callers can switch backends via the USE_LAKEBASE flag with minimal code
duplication.

Connection model:
  - One short-lived asyncpg connection per query (graph endpoints are
    bursty and low-concurrency; pooling adds complexity).
  - OAuth credentials refreshed via /api/2.0/postgres/credentials when
    the cached token is within 5 minutes of expiry.

Driver choice:
  - asyncpg (Apache-2.0). Async-native — no thread bridging — and avoids
    the LGPL license that psycopg carries.

Spark SQL → Postgres compatibility:
  - Strips `<catalog>.<schema>.` table qualifiers (Postgres uses bare names).
  - Unquotes integer literals (`'12345'` → `12345`) so bigint comparisons
    work; string literals like `'customer'` are untouched.
  - Other window function / CTE / LEAST / GREATEST syntax is identical
    between Spark and Postgres for our queries.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Any

import asyncpg
import requests

from backend import config

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config (env-driven)
# ---------------------------------------------------------------------------
LAKEBASE_HOST = os.getenv("LAKEBASE_HOST", "")
LAKEBASE_DATABASE = os.getenv("LAKEBASE_DATABASE", "")
LAKEBASE_ENDPOINT_PATH = os.getenv("LAKEBASE_ENDPOINT_PATH", "")  # e.g. projects/x/branches/production/endpoints/primary
LAKEBASE_USER = os.getenv("LAKEBASE_USER", "")  # set to SP/email; falls back to current-user lookup if empty

# ---------------------------------------------------------------------------
# Postgres credential cache
# ---------------------------------------------------------------------------
_pg_token: str | None = None
_pg_token_expiry: float = 0.0
_pg_user: str | None = None
_pg_lock = asyncio.Lock()


def _fetch_pg_credential() -> tuple[str, float]:
    """POST /api/2.0/postgres/credentials with workspace OAuth → Postgres OAuth token."""
    workspace_token = config.get_oauth_token()
    resp = requests.post(
        f"{config.DATABRICKS_WORKSPACE_URL}/api/2.0/postgres/credentials",
        headers={"Authorization": f"Bearer {workspace_token}"},
        json={"endpoint": LAKEBASE_ENDPOINT_PATH},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    token = data["token"]
    expire_iso = data["expire_time"].replace("Z", "+00:00")
    expiry = datetime.fromisoformat(expire_iso).timestamp()
    return token, expiry


def _resolve_user() -> str:
    """Return the username for the Postgres connection (SP principal or current user)."""
    if LAKEBASE_USER:
        return LAKEBASE_USER
    workspace_token = config.get_oauth_token()
    resp = requests.get(
        f"{config.DATABRICKS_WORKSPACE_URL}/api/2.0/preview/scim/v2/Me",
        headers={"Authorization": f"Bearer {workspace_token}"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["userName"]


async def _get_credentials() -> tuple[str, str]:
    """Return (user, token), refreshing if within 5 min of expiry."""
    global _pg_token, _pg_token_expiry, _pg_user
    async with _pg_lock:
        if _pg_user is None:
            _pg_user = await asyncio.to_thread(_resolve_user)
        if _pg_token is None or time.time() > (_pg_token_expiry - 300):
            _logger.info("🔑 Refreshing Lakebase credential…")
            _pg_token, _pg_token_expiry = await asyncio.to_thread(_fetch_pg_credential)
        return _pg_user, _pg_token


# ---------------------------------------------------------------------------
# Spark SQL → Postgres rewriter
# ---------------------------------------------------------------------------
_QUALIFIED_TABLE_RE = re.compile(r"\b\w+\.\w+\.(graph_nodes|graph_edges)\b")
_QUOTED_INT_RE = re.compile(r"'(\d+)'")


def _spark_to_postgres(sql: str) -> str:
    """Lightweight rewrite: strip catalog.schema prefix; unquote integer literals."""
    sql = _QUALIFIED_TABLE_RE.sub(r"\1", sql)
    sql = _QUOTED_INT_RE.sub(r"\1", sql)
    return sql


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------
async def execute_query(sql: str) -> list[dict[str, Any]]:
    """Run a SQL string against Lakebase and return rows as list[dict].

    Properties columns (JSONB in Postgres) are re-stringified to match the
    SQL-warehouse contract — callers downstream parse them with json.loads.
    """
    rewritten = _spark_to_postgres(sql)
    user, token = await _get_credentials()

    conn = await asyncpg.connect(
        host=LAKEBASE_HOST,
        port=5432,
        database=LAKEBASE_DATABASE,
        user=user,
        password=token,
        ssl="require",
        timeout=10,
    )
    try:
        # asyncpg returns jsonb as a parsed dict/list by default. The rest of
        # the codebase expects `properties` as a JSON string, so register a
        # codec that hands jsonb back as raw text.
        await conn.set_type_codec(
            "jsonb",
            encoder=lambda v: v,
            decoder=lambda v: v,
            schema="pg_catalog",
            format="text",
        )
        records = await conn.fetch(rewritten)
        return [dict(r) for r in records]
    finally:
        await conn.close()


def is_configured() -> bool:
    """Return True if all required Lakebase env vars are present."""
    return bool(LAKEBASE_HOST and LAKEBASE_DATABASE and LAKEBASE_ENDPOINT_PATH)


async def route_query(db_service, sql: str) -> list[dict[str, Any]]:
    """Dispatch a graph_nodes/graph_edges query to Lakebase or the SQL warehouse.

    Argument order matches `db_service.execute_query` ergonomics: callers can
    swap `db_service.execute_query(...)` → `lakebase_service.route_query(db_service, ...)`
    with a single global rename. When USE_LAKEBASE is on and Lakebase is
    configured, Postgres serves the read.

    If Lakebase raises for any reason (cold start, auth blip, query incompat),
    we log a warning and silently fall back to the SQL warehouse path so the
    user sees a slightly slower load instead of the demo placeholder. The
    warning log makes the failure visible so we can fix it before the next demo.
    """
    if config.USE_LAKEBASE and is_configured():
        try:
            return await execute_query(sql)
        except Exception as e:
            _logger.warning(
                "⚠️ Lakebase query failed, falling back to Delta: %s: %s",
                type(e).__name__, e,
            )
            return await db_service.execute_query(sql)
    return await db_service.execute_query(sql)
