"""Replay the FINS-AML agent graph into a target Databricks workspace.

Reads JSON files under ./agents/ (produced by export_agents.py) and
idempotently creates the You.com MCP connection, 3 Knowledge Assistants,
2 Genie Spaces, and the Multi-Agent Supervisor that ties them together.

Designed to run AFTER the data bundle has populated:
  - The tables under <catalog>.<schema>.* (cases, customers, alerts, ...)
  - The volume <catalog>.<schema>.knowledge_base/ (PDFs, txt, narratives, ...)

Dry-run by default. Pass --apply to actually mutate the workspace.

Usage:
    python provision_agents.py \\
        --profile <target-profile> \\
        --catalog fins_aml \\
        --schema data_generation \\
        --warehouse-id <warehouse_id> \\
        --mcp-secret-scope youcom \\
        --mcp-secret-key api_key \\
        --apply

The script discovers existing resources by name, so re-running is safe.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError


# When run as a Databricks Jobs spark_python_task, the script is exec'd
# without `__file__` being set. Fall back to cwd in that case — the
# bundle uploader puts the agents/ folder next to this script.
try:
    AGENTS_DIR = Path(__file__).parent / "agents"
except NameError:
    AGENTS_DIR = Path.cwd() / "agents"
SOURCE_CATALOG = "fins_aml"
SOURCE_SCHEMA = "data_generation"
KA_INDEX_TIMEOUT_SEC = 30 * 60
KA_INDEX_POLL_SEC = 15


def log(msg: str) -> None:
    print(msg, flush=True)


def load_jsons(subdir: str) -> list[dict[str, Any]]:
    path = AGENTS_DIR / subdir
    if not path.exists():
        return []
    return [json.loads(p.read_text()) for p in sorted(path.glob("*.json"))]


def retarget_volume_path(path: str, catalog: str, schema: str) -> str:
    """Rewrite /Volumes/<src_cat>/<src_sch>/... to use the target catalog/schema."""
    pattern = f"/Volumes/{SOURCE_CATALOG}/{SOURCE_SCHEMA}/"
    replacement = f"/Volumes/{catalog}/{schema}/"
    return path.replace(pattern, replacement)


def retarget_table_id(table_id: str, catalog: str, schema: str) -> str:
    """Rewrite <src_cat>.<src_sch>.table to use target catalog/schema."""
    parts = table_id.split(".")
    if len(parts) == 3 and parts[0] == SOURCE_CATALOG and parts[1] == SOURCE_SCHEMA:
        return f"{catalog}.{schema}.{parts[2]}"
    return table_id


# ---------------------------------------------------------------------------
# Resource lookups (find-by-name idempotency)
# ---------------------------------------------------------------------------

def find_tile_by_name(w: WorkspaceClient, name: str, tile_type: str) -> dict | None:
    res = w.api_client.do("GET", f"/api/2.0/tiles?tile_type={tile_type}")
    for tile in res.get("tiles", []):
        if tile.get("name") == name:
            return tile
    return None


def find_data_room_by_name(w: WorkspaceClient, display_name: str) -> dict | None:
    res = w.api_client.do("GET", "/api/2.0/data-rooms")
    for room in res.get("data_rooms", []):
        if room.get("display_name") == display_name:
            return room
    return None


def find_connection(w: WorkspaceClient, name: str) -> dict | None:
    try:
        return w.api_client.do("GET", f"/api/2.1/unity-catalog/connections/{name}")
    except DatabricksError as e:
        if "not exist" in str(e).lower() or "not found" in str(e).lower():
            return None
        raise


# ---------------------------------------------------------------------------
# Resource ensures
# ---------------------------------------------------------------------------

def ensure_mcp_connection(
    w: WorkspaceClient,
    spec: dict,
    bearer_token: str,
    apply: bool,
) -> str:
    name = spec["connection_name"]
    existing = find_connection(w, name)
    if existing:
        log(f"  [skip] MCP connection '{name}' already exists")
        return name

    opts = spec["connection"]["options"]
    body = {
        "name": name,
        "connection_type": "HTTP",
        "options": {
            "host": opts["host"],
            "port": opts.get("port", "443"),
            "base_path": opts.get("base_path", "/"),
            "auth_scheme": opts.get("auth_scheme", "bearer"),
            "bearer_token": bearer_token,
            "is_mcp_connection": "true",
        },
        "read_only": True,
        "comment": spec["connection"].get("comment", ""),
    }
    log(f"  [create] MCP connection '{name}' (host={opts['host']})")
    if not apply:
        return name
    w.api_client.do("POST", "/api/2.1/unity-catalog/connections", body=body)
    return name


def ensure_knowledge_assistant(
    w: WorkspaceClient,
    spec: dict,
    catalog: str,
    schema: str,
    apply: bool,
) -> str:
    name = spec["name"]
    existing = find_tile_by_name(w, name, "KA")
    if existing:
        log(f"  [skip] KA '{name}' already exists (tile_id={existing['tile_id']})")
        return existing["tile_id"]

    sources = []
    for src in spec["knowledge_sources"]:
        retargeted_path = retarget_volume_path(src["files"]["path"], catalog, schema)
        sources.append({
            "files_source": {
                "name": src["name"],
                "description": src.get("description", ""),
                "type": src.get("type", "files"),
                "files": {"path": retargeted_path},
            }
        })

    body = {
        "name": name,
        "description": spec["description"],
        "instructions": spec.get("instructions", ""),
        "knowledge_sources": sources,
    }
    log(f"  [create] KA '{name}' with {len(sources)} source(s):")
    for s in sources:
        log(f"           - {s['files_source']['name']}: {s['files_source']['files']['path']}")
    if not apply:
        return ""

    res = w.api_client.do("POST", "/api/2.0/knowledge-assistants", body=body)
    tile_id = res["knowledge_assistant"]["id"]
    log(f"           tile_id={tile_id}")
    _wait_for_ka_indexing(w, tile_id, name)
    return tile_id


def _wait_for_ka_indexing(w: WorkspaceClient, tile_id: str, name: str) -> None:
    log(f"           waiting for indexing (timeout {KA_INDEX_TIMEOUT_SEC // 60}min)…")
    deadline = time.time() + KA_INDEX_TIMEOUT_SEC
    while time.time() < deadline:
        ka = w.api_client.do("GET", f"/api/2.0/knowledge-assistants/{tile_id}")["knowledge_assistant"]
        states = [s.get("state") for s in ka.get("knowledge_sources", [])]
        if states and all(s == "KNOWLEDGE_SOURCE_STATE_UPDATED" for s in states):
            log(f"           ✅ KA '{name}' indexed ({len(states)} sources)")
            return
        time.sleep(KA_INDEX_POLL_SEC)
    raise TimeoutError(f"KA '{name}' did not finish indexing within timeout")


def ensure_genie_space(
    w: WorkspaceClient,
    spec: dict,
    catalog: str,
    schema: str,
    warehouse_id: str,
    apply: bool,
) -> str:
    display_name = spec["data_room"]["display_name"]
    description = spec["data_room"].get("description", "")
    src_tables = spec["data_room"].get("table_identifiers", [])
    tables = [retarget_table_id(t, catalog, schema) for t in src_tables]

    existing = find_data_room_by_name(w, display_name)
    if existing:
        log(f"  [skip] Genie '{display_name}' already exists (id={existing['space_id']})")
        return existing["space_id"]

    log(f"  [create] Genie space '{display_name}' with {len(tables)} table(s)")
    for t in tables:
        log(f"           - {t}")
    if not apply:
        return ""

    create_body = {"display_name": display_name, "warehouse_id": warehouse_id}
    created = w.api_client.do("POST", "/api/2.0/data-rooms", body=create_body)
    space_id = created["space_id"]
    log(f"           space_id={space_id}")

    patch_body = {
        "display_name": display_name,
        "warehouse_id": warehouse_id,
        "description": description,
        "table_identifiers": tables,
    }
    w.api_client.do("PATCH", f"/api/2.0/data-rooms/{space_id}", body=patch_body)
    log(f"           attached {len(tables)} table(s)")
    return space_id


def ensure_mas(
    w: WorkspaceClient,
    mas_spec: dict,
    sub_agent_ids: dict[str, str],
    apply: bool,
) -> str:
    mas = mas_spec["multi_agent_supervisor"]
    tile = mas["tile"]
    name = tile["name"]

    existing = find_tile_by_name(w, name, "MAS")
    if existing:
        log(f"  [skip] MAS '{name}' already exists (tile_id={existing['tile_id']})")
        return existing["tile_id"]

    agents_payload = []
    for agent in mas["agents"]:
        a = {"name": agent["name"], "description": agent["description"]}
        atype = agent["agent_type"]
        if atype == "knowledge-assistant":
            # Map by sub-agent name → newly-created KA serving endpoint
            endpoint_name = sub_agent_ids.get(f"ka:{agent['name']}")
            if not endpoint_name:
                raise RuntimeError(f"Missing KA endpoint for sub-agent {agent['name']}")
            a["serving_endpoint"] = {"name": endpoint_name}
        elif atype == "genie-space":
            space_id = sub_agent_ids.get(f"genie:{agent['name']}")
            if not space_id:
                raise RuntimeError(f"Missing Genie space for sub-agent {agent['name']}")
            a["genie_space"] = {"id": space_id}
        elif atype == "external-mcp-server":
            conn_name = sub_agent_ids.get(f"mcp:{agent['name']}")
            if not conn_name:
                raise RuntimeError(f"Missing MCP connection for sub-agent {agent['name']}")
            a["external_mcp_server"] = {"connection_name": conn_name}
        else:
            raise RuntimeError(f"Unknown agent_type: {atype}")
        agents_payload.append(a)

    body = {
        "name": name,
        "description": tile["description"],
        "instructions": tile.get("instructions", ""),
        "agents": agents_payload,
    }
    log(f"  [create] MAS '{name}' wiring {len(agents_payload)} sub-agents")
    for a in agents_payload:
        log(f"           - {a['name']}")
    if not apply:
        return ""

    res = w.api_client.do("POST", "/api/2.0/multi-agent-supervisors", body=body)
    tile_id = res["multi_agent_supervisor"]["tile"]["tile_id"]
    log(f"           tile_id={tile_id}")
    return tile_id


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", help="Databricks CLI profile (omit if running in workspace)")
    parser.add_argument("--catalog", required=True, help="Target catalog (e.g. fins_aml)")
    parser.add_argument("--schema", required=True, help="Target schema (e.g. data_generation)")
    parser.add_argument("--warehouse-id", required=True, help="Warehouse for Genie spaces")
    parser.add_argument("--mcp-secret-scope", help="Secret scope for You.com bearer token (omit + --skip-mcp to skip)")
    parser.add_argument("--mcp-secret-key", help="Secret key for You.com bearer token (omit + --skip-mcp to skip)")
    parser.add_argument("--skip-mcp", action="store_true", help="Skip You.com MCP setup; MAS will be created without it")
    parser.add_argument("--apply", action="store_true", help="Actually create resources (default: dry-run)")
    args = parser.parse_args()

    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()

    if not args.apply:
        log("=== DRY RUN === (pass --apply to actually create resources)\n")

    # Auto-skip MCP if secret args are empty (lets bundle pass empty strings)
    if not (args.mcp_secret_scope and args.mcp_secret_key):
        args.skip_mcp = True

    bearer_token = ""
    if args.apply and not args.skip_mcp:
        log("Reading You.com bearer token from secret…")
        import base64
        raw = w.secrets.get_secret(scope=args.mcp_secret_scope, key=args.mcp_secret_key).value
        bearer_token = base64.b64decode(raw).decode()
        log(f"  got token ({len(bearer_token)} chars)\n")
    elif args.skip_mcp:
        log("(--skip-mcp: skipping You.com MCP setup; MAS will exclude it)\n")
    else:
        log(f"(skipping secret read in dry-run; would read {args.mcp_secret_scope}/{args.mcp_secret_key})\n")

    sub_agent_ids: dict[str, str] = {}

    log("Step 1: MCP connections")
    if args.skip_mcp:
        log("  [skip] --skip-mcp set")
    else:
        for mcp_spec in load_jsons("mcp"):
            conn_name = ensure_mcp_connection(w, mcp_spec, bearer_token, args.apply)
            sub_agent_ids[f"mcp:{mcp_spec['name']}"] = conn_name

    log("\nStep 2: Knowledge Assistants (may take minutes per KA to index)")
    for ka_spec in load_jsons("kas"):
        ensure_knowledge_assistant(w, ka_spec, args.catalog, args.schema, args.apply)
        # Endpoint name = ka-<short_tile_id>-endpoint; we discover by listing
        if args.apply:
            tile = find_tile_by_name(w, ka_spec["name"], "KA")
            sub_agent_ids[f"ka:{ka_spec['name']}"] = tile["serving_endpoint_name"]

    log("\nStep 3: Genie Spaces")
    for genie_spec in load_jsons("genies"):
        space_id = ensure_genie_space(w, genie_spec, args.catalog, args.schema, args.warehouse_id, args.apply)
        sub_agent_ids[f"genie:{genie_spec['name']}"] = space_id

    log("\nStep 4: Multi-Agent Supervisor")
    mas_spec = json.loads((AGENTS_DIR / "mas.json").read_text())
    if args.skip_mcp:
        # Drop external-mcp-server agents from the MAS spec when MCP is skipped
        mas_spec["multi_agent_supervisor"]["agents"] = [
            a for a in mas_spec["multi_agent_supervisor"]["agents"]
            if a.get("agent_type") != "external-mcp-server"
        ]
    ensure_mas(w, mas_spec, sub_agent_ids, args.apply)

    log("\nDone.")
    if not args.apply:
        log("Re-run with --apply to execute.")


if __name__ == "__main__":
    sys.exit(main())
