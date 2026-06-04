"""Export the live FINS-AML agent graph (MAS + sub-agents) to JSON.

Run against the workspace that currently hosts the agents (see
the --profile flag). Output lands in ./agents/ as a set of JSON
files that provision_agents.py consumes to recreate the same
graph in any target workspace.

Usage:
    python export_agents.py \\
        --profile <your-cli-profile> \\
        --mas-tile-id <mas-tile-id-from-source-workspace>

What's captured:
  - mas.json                          MAS supervisor + agents array
  - kas/<short>.json                  Per-KA tile metadata
  - genies/<short>.json               Per-Genie data-room config (tables, description)
  - mcp/<short>.json                  UC connection for external MCP server
                                      (bearer token NOT included)

What's NOT captured (must be re-applied out of band):
  - MAS guidelines / ALHF feedback    UI-only, no REST surface
  - You.com bearer token              Must be set as a Databricks secret
                                      in the target workspace
  - KA source documents               Produced by data bundle notebook 04
                                      into /Volumes/<catalog>/<schema>/knowledge_base
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient


AGENTS_DIR = Path(__file__).parent / "agents"


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(f"  wrote {path.relative_to(AGENTS_DIR.parent)}")


def api_get(w: WorkspaceClient, path: str) -> dict[str, Any]:
    return w.api_client.do("GET", path)


def export_mas(w: WorkspaceClient, tile_id: str) -> dict[str, Any]:
    print(f"Exporting MAS {tile_id}")
    mas = api_get(w, f"/api/2.0/multi-agent-supervisors/{tile_id}")
    write_json(AGENTS_DIR / "mas.json", mas)
    return mas


def export_kas(w: WorkspaceClient, agents: list[dict[str, Any]]) -> None:
    for agent in agents:
        if agent.get("agent_type") != "knowledge-assistant":
            continue
        endpoint_name = agent["serving_endpoint"]["name"]
        endpoint = api_get(w, f"/api/2.0/serving-endpoints/{endpoint_name}")
        tile_id = endpoint["tile_endpoint_metadata"]["tile_id"]
        ka_full = api_get(w, f"/api/2.0/knowledge-assistants/{tile_id}")["knowledge_assistant"]

        # Strip workspace-specific source info; keep only what's needed
        # to reconstruct the KA in a target workspace.
        sources = []
        for src in ka_full.get("knowledge_sources", []):
            files_source = src.get("files_source") or {}
            sources.append({
                "name": files_source.get("name"),
                "description": files_source.get("description"),
                "type": files_source.get("type"),
                "files": files_source.get("files"),  # {"path": "/Volumes/.../..."}
            })

        payload = {
            "name": agent["name"],
            "description": agent["description"],
            "instructions": ka_full.get("instructions", ""),
            "knowledge_sources": sources,
            "_source_endpoint": endpoint_name,
            "_source_tile_id": tile_id,
        }
        write_json(AGENTS_DIR / "kas" / f"{slugify(agent['name'])}.json", payload)


def export_genies(w: WorkspaceClient, agents: list[dict[str, Any]]) -> None:
    for agent in agents:
        if agent.get("agent_type") != "genie-space":
            continue
        space_id = agent["genie_space"]["id"]
        space = api_get(w, f"/api/2.0/data-rooms/{space_id}")
        payload = {
            "name": agent["name"],
            "description": agent["description"],
            "space_id": space_id,
            "data_room": space,
        }
        write_json(AGENTS_DIR / "genies" / f"{slugify(agent['name'])}.json", payload)


def export_mcp_servers(w: WorkspaceClient, agents: list[dict[str, Any]]) -> None:
    for agent in agents:
        if agent.get("agent_type") != "external-mcp-server":
            continue
        connection_name = agent["external_mcp_server"]["connection_name"]
        conn = api_get(w, f"/api/2.1/unity-catalog/connections/{connection_name}")
        payload = {
            "name": agent["name"],
            "description": agent["description"],
            "connection_name": connection_name,
            "connection": {
                "connection_type": conn.get("connection_type"),
                "credential_type": conn.get("credential_type"),
                "options": conn.get("options", {}),
                "comment": conn.get("comment"),
                "read_only": conn.get("read_only"),
            },
        }
        write_json(AGENTS_DIR / "mcp" / f"{slugify(agent['name'])}.json", payload)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True, help="Databricks CLI profile to use")
    parser.add_argument("--mas-tile-id", required=True, help="MAS tile_id to export from")
    args = parser.parse_args()

    w = WorkspaceClient(profile=args.profile)
    AGENTS_DIR.mkdir(exist_ok=True)

    mas = export_mas(w, args.mas_tile_id)
    agents = mas["multi_agent_supervisor"]["agents"]
    print(f"Found {len(agents)} sub-agents")

    export_kas(w, agents)
    export_genies(w, agents)
    export_mcp_servers(w, agents)

    print("\nDone. Review files under ./agents/ and commit.")


if __name__ == "__main__":
    main()
