"""Semantic graph search API — Vector Search + subgraph retrieval + FM summary"""

import logging
import json
import asyncio
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, Dict, List, Any

from backend.services.database import DatabaseService
from backend.services import lakebase as lakebase_service
from backend import config

router = APIRouter()
logger = logging.getLogger(__name__)

# Vector Search configuration
VS_ENDPOINT_NAME = "fins-aml-vs-endpoint"
VS_INDEX_NAME = f"{config.CATALOG}.{config.SCHEMA}.graph_node_embeddings_index"

# AI Gateway model for summaries (workspace ID injected by Databricks Apps runtime)
AI_GATEWAY_BASE_URL = f"https://{config.DATABRICKS_WORKSPACE_ID}.ai-gateway.cloud.databricks.com/mlflow/v1"
AI_GATEWAY_MODEL = "fins-aml-claude-sonnet-46"


async def get_db_service() -> DatabaseService:
    from main import db_service
    if not db_service:
        raise HTTPException(status_code=503, detail="Database service unavailable")
    return db_service


def _rewrite_query(user_query: str) -> Dict:
    """Use Sonnet to rewrite the user query into optimized search text + metadata filter."""
    from openai import OpenAI
    import json

    token = config.get_oauth_token()
    client = OpenAI(api_key=token, base_url=AI_GATEWAY_BASE_URL)

    try:
        response = client.chat.completions.create(
            model=AI_GATEWAY_MODEL,
            messages=[{"role": "user", "content": f"""You are a search query optimizer for a knowledge graph containing AML (Anti-Money Laundering) entities.

The graph has these node types: customer, account, alert, counterparty, watchlist.

These are the AML detection scenarios (alert types) in the data — match user queries to these even if they use synonyms or paraphrasing:
- Beneficiary Mismatch (mismatched payee, wrong recipient)
- Cash Structuring Detection (structuring, smurfing, deposits below CTR threshold)
- Dormant Account Reactivation (inactive account, dormant, reactivated)
- High-Risk Geography Transfer (offshore, sanctioned country, high-risk jurisdiction, foreign transfers)
- PEP/Sanctions Alert (politically exposed person, sanctions list, OFAC, watchlist hit)
- Rapid Fund Movement (rapid transfers, quick in-out, fast movement of funds)
- Related Account Movement (linked accounts, related parties, same-address transfers)
- Round Dollar Pattern (round amounts, even dollar, repetitive round transfers)
- Third-Party Deposit Pattern (third party deposits, non-account holder deposits, 3rd party cash)

Given a user's natural language query, determine the intent and output a JSON object with:
- "mode": either "lookup" (user is asking about a specific named entity, e.g. "Brianna Alexander", "show me James Torres") or "search" (user is asking a general question like "high risk customers with offshore connections")
- "entity_name": if mode is "lookup", the exact entity name to look up. Otherwise empty string.
- "search_text": if mode is "search", an optimized search string. Otherwise empty string.
- "node_type": the SINGLE most relevant node type that the user WANTS TO SEE in results, or "all". IMPORTANT: if the user says "customers flagged for X" or "customers involved in Y", the node_type MUST be "customer" because they want to see customers, not alerts or other entities.

User query: "{user_query}"

Respond with ONLY valid JSON, no explanation."""}],
            max_tokens=150,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            raw = raw.rsplit("```", 1)[0]
        parsed = json.loads(raw)
        logger.info(f"Query rewrite: '{user_query}' -> mode={parsed.get('mode')}, entity={parsed.get('entity_name', '')}, search={parsed.get('search_text', '')}, type={parsed.get('node_type', 'all')}")
        return parsed
    except Exception as e:
        logger.warning(f"Query rewrite failed ({e}), using original query")
        return {"mode": "search", "search_text": user_query, "entity_name": "", "node_type": "all"}


def _search_vector_index(query: str, num_results: int = 10, search_text: str = None, node_type: str = "all") -> List[Dict]:
    """Query the Vector Search index using service principal auth."""
    import os
    from databricks.vector_search.client import VectorSearchClient

    # Use pre-processed search text if provided, otherwise use raw query
    search_text = search_text or query

    vsc = VectorSearchClient(
        workspace_url=config.DATABRICKS_WORKSPACE_URL,
        service_principal_client_id=os.getenv("DATABRICKS_CLIENT_ID"),
        service_principal_client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
    )
    index = vsc.get_index(VS_ENDPOINT_NAME, VS_INDEX_NAME)

    # Step 2: Build filters if specific node types were identified
    search_kwargs = {
        "columns": ["node_id", "node_type", "node_label", "risk_score",
                     "risk_category", "text_description"],
        "query_text": search_text,
        "num_results": num_results,
    }

    if node_type and node_type != "all":
        search_kwargs["filters"] = {"node_type": node_type}

    results = index.similarity_search(**search_kwargs)

    matches = []
    for row in results.get("result", {}).get("data_array", []):
        matches.append({
            "node_id": row[0],
            "node_type": row[1],
            "node_label": row[2],
            "risk_score": row[3],
            "risk_category": row[4],
            "text_description": row[5],
        })
    return matches


def _generate_summary(query: str, matches: List[Dict]) -> str:
    """Generate a brief plain-text summary using the AI Gateway."""
    from openai import OpenAI

    token = config.get_oauth_token()
    client = OpenAI(api_key=token, base_url=AI_GATEWAY_BASE_URL)

    top_matches = "\n".join([
        f"- {m['node_type']}: {m['node_label']} (risk: {m['risk_score']})"
        for m in matches[:5]
    ])

    prompt = f"""Search query: "{query}"
Top matches:
{top_matches}

Write 2-3 short sentences. Name the highest-risk entity and suggest one next step. Do NOT mention how many results were returned or reference counts. No markdown, no headers, plain text only. Be very brief."""

    try:
        response = client.chat.completions.create(
            model=AI_GATEWAY_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=120,
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.warning(f"AI summary failed: {e}")
        return f"Found {len(matches)} entities matching your query. Review the graph visualization for relationship details."


@router.get("/search")
async def semantic_graph_search(
    q: str = Query(..., description="Natural language search query"),
    limit: int = Query(default=10, ge=1, le=50, description="Max matching nodes"),
    depth: int = Query(default=1, ge=1, le=2, description="Subgraph traversal depth"),
    db_service: DatabaseService = Depends(get_db_service),
):
    """
    Semantic search over the knowledge graph.

    1. Embeds the query and searches the Vector Search index for matching nodes
    2. Pulls 1-hop subgraphs for each matched node
    3. Generates a natural language summary using a Foundation Model
    4. Returns graph JSON + summary for the frontend
    """
    try:
        logger.info(f"Semantic search: '{q}' (limit={limit}, depth={depth})")

        # Step 1: Rewrite query and determine mode
        rewritten = await asyncio.to_thread(_rewrite_query, q)
        mode = rewritten.get("mode", "search")
        entity_name = rewritten.get("entity_name", "")

        # LOOKUP MODE: direct entity query by name
        if mode == "lookup" and entity_name:
            logger.info(f"Lookup mode for entity: '{entity_name}'")
            # Find the customer by name
            customer_query = f"""
            SELECT node_id, node_type, node_label, risk_score, risk_category, properties
            FROM {config.CATALOG}.{config.SCHEMA}.graph_nodes
            WHERE LOWER(node_label) LIKE LOWER('%{entity_name}%')
            LIMIT 1
            """
            customer_result = await lakebase_service.route_query(db_service,customer_query)
            if not customer_result:
                return {"nodes": [], "relationships": [], "matches": [], "summary": f"No entity found matching '{entity_name}'.", "query": q}

            c = customer_result[0]
            matches = [{
                "node_id": str(c["node_id"]), "node_type": c["node_type"], "node_label": c["node_label"],
                "risk_score": c["risk_score"], "risk_category": c["risk_category"],
                "text_description": f"{c['node_type']}: {c['node_label']} | Risk: {c['risk_category']} (score {c['risk_score']})"
            }]
            # Pull ALL edges for this single entity
            target_id = c["node_id"]
            target_type = c["node_type"]
            edges_query = f"""
            WITH ranked AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY LEAST(source_node_id, target_node_id),
                                 GREATEST(source_node_id, target_node_id), edge_type
                    ORDER BY weight DESC
                ) AS rn
                FROM {config.CATALOG}.{config.SCHEMA}.graph_edges
                WHERE (source_node_id = '{target_id}' AND source_node_type = '{target_type}')
                   OR (target_node_id = '{target_id}' AND target_node_type = '{target_type}')
            )
            SELECT * FROM ranked WHERE rn = 1 ORDER BY weight DESC LIMIT 200
            """
            edges_result = await lakebase_service.route_query(db_service,edges_query)

            all_node_ids = {(target_type, str(target_id))}
            for edge in edges_result:
                all_node_ids.add((edge["source_node_type"], str(edge["source_node_id"])))
                all_node_ids.add((edge["target_node_type"], str(edge["target_node_id"])))

            node_conditions = " OR ".join([f"(node_type = '{nt}' AND node_id = '{ni}')" for nt, ni in all_node_ids])
            nodes_result = await lakebase_service.route_query(db_service,f"""
                SELECT DISTINCT node_id, node_type, node_label, risk_score, risk_category, properties
                FROM {config.CATALOG}.{config.SCHEMA}.graph_nodes WHERE {node_conditions}
            """)

            node_color_map = {"customer": "#1D4ED8", "alert": "#BD2B26", "account": "#7FB3D5", "counterparty": "#D4AF37", "watchlist": "#730D21", "transaction": "#64748B"}
            nodes = [{"id": f"{n['node_type']}_{n['node_id']}", "type": n["node_type"], "label": n.get("node_label", ""),
                      "color": node_color_map.get(n["node_type"], "#64748B"), "isMatch": str(n["node_id"]) == str(target_id),
                      "riskScore": n.get("risk_score", 0), "riskCategory": n.get("risk_category", "unknown")} for n in nodes_result]
            relationships = [{"id": f"r{i}", "from": f"{e['source_node_type']}_{e['source_node_id']}",
                              "to": f"{e['target_node_type']}_{e['target_node_id']}", "type": e.get("edge_type", "RELATED")} for i, e in enumerate(edges_result)]

            summary = f"{c['node_label']} ({c['node_type']}) — risk score {c['risk_score']} ({c['risk_category']}). Showing all {len(relationships)} direct relationships."
            return {"nodes": nodes, "relationships": relationships, "matches": matches, "summary": summary,
                    "query": q, "stats": {"total_nodes": len(nodes), "total_edges": len(relationships)}}

        # SEARCH MODE: vector search with rewritten query
        search_text = rewritten.get("search_text", q)
        node_type = rewritten.get("node_type", "all")
        matches = await asyncio.to_thread(_search_vector_index, q, limit, search_text, node_type)

        if not matches:
            return {"nodes": [], "relationships": [], "matches": [], "summary": "No matching entities found. Try a different search query.", "query": q}

        # If matches are mostly non-customer types (alerts, watchlists, etc.),
        # resolve to their connected customers — customers make for richer graphs
        # and are what the user typically wants to investigate
        non_customer_matches = [m for m in matches if m["node_type"] not in ("customer", "counterparty")]
        if len(non_customer_matches) > len(matches) * 0.5:
            logger.info("Resolving non-customer matches to connected customers")
            non_customer_ids = [(m["node_type"], m["node_id"]) for m in non_customer_matches]
            resolve_conditions = " OR ".join([
                f"((source_node_id = '{nid}' AND source_node_type = '{ntype}' AND target_node_type = 'customer')"
                f" OR (target_node_id = '{nid}' AND target_node_type = '{ntype}' AND source_node_type = 'customer'))"
                for ntype, nid in non_customer_ids
            ])
            resolve_query = f"""
            SELECT DISTINCT n.node_id, n.node_type, n.node_label, n.risk_score, n.risk_category, n.properties
            FROM {config.CATALOG}.{config.SCHEMA}.graph_edges e
            JOIN {config.CATALOG}.{config.SCHEMA}.graph_nodes n
              ON (n.node_id = e.source_node_id AND n.node_type = 'customer' AND ({resolve_conditions}))
              OR (n.node_id = e.target_node_id AND n.node_type = 'customer' AND ({resolve_conditions}))
            WHERE n.node_type = 'customer'
            LIMIT 30
            """
            try:
                resolved = await lakebase_service.route_query(db_service,resolve_query)
                if resolved:
                    # Replace matches with the resolved customers — the original
                    # non-customer matches (watchlist/alert) were intermediaries
                    customer_matches = [m for m in matches if m["node_type"] in ("customer", "counterparty")]
                    existing_ids = {m["node_id"] for m in customer_matches}
                    for r in resolved:
                        rid = str(r["node_id"])
                        if rid not in existing_ids:
                            customer_matches.append({
                                "node_id": rid, "node_type": "customer", "node_label": r["node_label"],
                                "risk_score": r["risk_score"], "risk_category": r["risk_category"],
                                "text_description": f"Customer: {r['node_label']} | Risk: {r['risk_category']} (score {r['risk_score']})"
                            })
                            existing_ids.add(rid)
                    matches = customer_matches
                    logger.info(f"Resolved to {len(matches)} customers (replaced non-customer matches)")
            except Exception as e:
                logger.warning(f"Customer resolution failed: {e}")

        # Sort matches by risk score descending
        matches.sort(key=lambda m: float(m.get("risk_score", 0) or 0), reverse=True)

        logger.info(f"Vector Search returned {len(matches)} matches")

        # Start summary generation in parallel with subgraph query
        summary_task = asyncio.create_task(
            asyncio.to_thread(_generate_summary, q, matches)
        )

        # Step 2: Pull subgraphs — use only customer/counterparty matches for the graph
        # (watchlist/alert nodes have few edges and consume budget without adding value)
        graph_worthy_types = ("customer", "counterparty")
        graph_matches = [m for m in matches if m["node_type"] in graph_worthy_types]
        if not graph_matches:
            graph_matches = matches[:10]  # fallback if no customers
        matched_node_ids = [(m["node_type"], m["node_id"]) for m in graph_matches]

        # First: get all edges connected to matched nodes
        edge_conditions = " OR ".join([
            f"(source_node_id = '{nid}' AND source_node_type = '{ntype}')"
            f" OR (target_node_id = '{nid}' AND target_node_type = '{ntype}')"
            for ntype, nid in matched_node_ids
        ])

        edges_query = f"""
        WITH direct_edges AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY LEAST(source_node_id, target_node_id),
                             GREATEST(source_node_id, target_node_id), edge_type
                ORDER BY weight DESC
            ) AS rn
            FROM {config.CATALOG}.{config.SCHEMA}.graph_edges
            WHERE {edge_conditions}
        ),
        first_hop AS (
            SELECT * FROM direct_edges WHERE rn = 1
        ),
        neighbor_ids AS (
            SELECT DISTINCT source_node_id AS nid, source_node_type AS ntype FROM first_hop
            UNION
            SELECT DISTINCT target_node_id, target_node_type FROM first_hop
        ),
        cross_edges AS (
            SELECT e.*, ROW_NUMBER() OVER (
                PARTITION BY LEAST(e.source_node_id, e.target_node_id),
                             GREATEST(e.source_node_id, e.target_node_id), e.edge_type
                ORDER BY e.weight DESC
            ) AS rn
            FROM {config.CATALOG}.{config.SCHEMA}.graph_edges e
            INNER JOIN neighbor_ids s ON e.source_node_id = s.nid AND e.source_node_type = s.ntype
            INNER JOIN neighbor_ids t ON e.target_node_id = t.nid AND e.target_node_type = t.ntype
        )
        SELECT * FROM first_hop
        UNION
        SELECT * FROM cross_edges WHERE rn = 1
        ORDER BY weight DESC
        LIMIT 500
        """

        edges_result = await lakebase_service.route_query(db_service,edges_query)

        # Collect all node IDs from edges + matched nodes
        all_node_ids = set(matched_node_ids)
        for edge in edges_result:
            all_node_ids.add((edge["source_node_type"], str(edge["source_node_id"])))
            all_node_ids.add((edge["target_node_type"], str(edge["target_node_id"])))

        # Fetch all nodes
        node_conditions = " OR ".join([
            f"(node_type = '{ntype}' AND node_id = '{nid}')"
            for ntype, nid in all_node_ids
        ])

        nodes_query = f"""
        SELECT DISTINCT node_id, node_type, node_label, risk_score, risk_category, properties
        FROM {config.CATALOG}.{config.SCHEMA}.graph_nodes
        WHERE {node_conditions}
        """

        nodes_result = await lakebase_service.route_query(db_service,nodes_query)

        # Format for frontend (same format as databricks_graph.py)
        node_color_map = {
            "customer": "#1D4ED8",
            "alert": "#BD2B26",
            "account": "#7FB3D5",
            "counterparty": "#D4AF37",
            "watchlist": "#730D21",
            "transaction": "#64748B",
        }

        matched_ids = {m["node_id"] for m in matches}

        nodes = []
        for node in nodes_result:
            ntype = node.get("node_type", "unknown")
            nid = str(node.get("node_id", ""))
            is_match = nid in matched_ids
            nodes.append({
                "id": f"{ntype}_{nid}",
                "type": ntype,
                "label": node.get("node_label", ""),
                "color": node_color_map.get(ntype, "#64748B"),
                "isMatch": is_match,  # Frontend can highlight matched nodes
                "riskScore": node.get("risk_score", 0),
                "riskCategory": node.get("risk_category", "unknown"),
            })

        relationships = []
        for idx, edge in enumerate(edges_result):
            relationships.append({
                "id": f"r{idx}",
                "from": f"{edge['source_node_type']}_{edge['source_node_id']}",
                "to": f"{edge['target_node_type']}_{edge['target_node_id']}",
                "type": edge.get("edge_type", "RELATED"),
            })

        # Step 3: Await the summary (was started in parallel with step 2)
        subgraph_stats = {"total_nodes": len(nodes), "total_edges": len(relationships)}
        summary = await summary_task

        logger.info(f"Returning {len(nodes)} nodes, {len(relationships)} edges")

        return {
            "nodes": nodes,
            "relationships": relationships,
            "matches": matches,
            "summary": summary,
            "query": q,
            "stats": subgraph_stats,
        }

    except Exception as e:
        logger.error(f"Semantic search error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)[:200]}")


@router.get("/overview")
async def graph_overview(
    top_customers: int = Query(default=20, ge=5, le=50),
    db_service: DatabaseService = Depends(get_db_service),
):
    """
    Return a large overview subgraph for the initial Graph Explorer view.

    Fetches the top-N highest-risk customers and all their 1-hop connections.
    """
    try:
        logger.info(f"Loading graph overview (top {top_customers} customers)")

        # Get top high-risk customers that have at least 5 connections
        customers_query = f"""
        WITH customer_edge_counts AS (
            SELECT n.node_id, n.node_type, n.node_label, n.risk_score, n.risk_category, n.properties,
                   COUNT(DISTINCT CONCAT(e.source_node_id, '-', e.target_node_id)) as edge_count
            FROM {config.CATALOG}.{config.SCHEMA}.graph_nodes n
            LEFT JOIN {config.CATALOG}.{config.SCHEMA}.graph_edges e
                ON (e.source_node_id = n.node_id AND e.source_node_type = 'customer')
                OR (e.target_node_id = n.node_id AND e.target_node_type = 'customer')
            WHERE n.node_type = 'customer'
            GROUP BY n.node_id, n.node_type, n.node_label, n.risk_score, n.risk_category, n.properties
            HAVING COUNT(DISTINCT CONCAT(e.source_node_id, '-', e.target_node_id)) >= 15
        )
        SELECT node_id, node_type, node_label, risk_score, risk_category, properties
        FROM customer_edge_counts
        ORDER BY risk_score DESC
        LIMIT {top_customers}
        """
        customers = await lakebase_service.route_query(db_service,customers_query)

        if not customers:
            return {"nodes": [], "relationships": [], "stats": {"total_nodes": 0, "total_edges": 0}}

        customer_ids = [str(c["node_id"]) for c in customers]

        # Get all edges for these customers (deduplicated)
        id_list = "','".join(customer_ids)
        edges_query = f"""
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY
                           LEAST(source_node_id, target_node_id),
                           GREATEST(source_node_id, target_node_id),
                           edge_type
                       ORDER BY weight DESC
                   ) AS rn
            FROM {config.CATALOG}.{config.SCHEMA}.graph_edges
            WHERE (source_node_id IN ('{id_list}') AND source_node_type = 'customer')
               OR (target_node_id IN ('{id_list}') AND target_node_type = 'customer')
        )
        SELECT * FROM ranked WHERE rn = 1
        ORDER BY weight DESC
        LIMIT 800
        """
        edges_result = await lakebase_service.route_query(db_service,edges_query)

        # Collect all node IDs
        all_node_ids = {("customer", cid) for cid in customer_ids}
        for edge in edges_result:
            all_node_ids.add((edge["source_node_type"], str(edge["source_node_id"])))
            all_node_ids.add((edge["target_node_type"], str(edge["target_node_id"])))

        # Fetch all nodes
        node_conditions = " OR ".join([
            f"(node_type = '{ntype}' AND node_id = '{nid}')"
            for ntype, nid in all_node_ids
        ])

        nodes_query = f"""
        SELECT DISTINCT node_id, node_type, node_label, risk_score, risk_category, properties
        FROM {config.CATALOG}.{config.SCHEMA}.graph_nodes
        WHERE {node_conditions}
        """
        nodes_result = await lakebase_service.route_query(db_service,nodes_query)

        # Format
        node_color_map = {
            "customer": "#1D4ED8", "alert": "#BD2B26", "account": "#7FB3D5",
            "counterparty": "#D4AF37", "watchlist": "#730D21", "transaction": "#64748B",
        }
        customer_id_set = set(customer_ids)

        nodes = []
        for node in nodes_result:
            ntype = node.get("node_type", "unknown")
            nid = str(node.get("node_id", ""))
            nodes.append({
                "id": f"{ntype}_{nid}",
                "type": ntype,
                "label": node.get("node_label", ""),
                "color": node_color_map.get(ntype, "#64748B"),
                "isMatch": ntype == "customer" and nid in customer_id_set,
                "riskScore": node.get("risk_score", 0),
                "riskCategory": node.get("risk_category", "unknown"),
            })

        relationships = []
        for idx, edge in enumerate(edges_result):
            relationships.append({
                "id": f"r{idx}",
                "from": f"{edge['source_node_type']}_{edge['source_node_id']}",
                "to": f"{edge['target_node_type']}_{edge['target_node_id']}",
                "type": edge.get("edge_type", "RELATED"),
            })

        logger.info(f"Overview: {len(nodes)} nodes, {len(relationships)} edges")

        return {
            "nodes": nodes,
            "relationships": relationships,
            "stats": {"total_nodes": len(nodes), "total_edges": len(relationships)},
            "summary": f"Showing the top {top_customers} highest-risk customers and their direct connections. Use the search bar to explore specific entities or patterns.",
            "matches": [{"node_id": c["node_id"], "node_type": "customer", "node_label": c["node_label"],
                         "risk_score": c["risk_score"], "risk_category": c["risk_category"],
                         "text_description": f"Customer: {c['node_label']} | Risk: {c['risk_category']} (score {c['risk_score']})"
                         } for c in customers],
        }

    except Exception as e:
        logger.error(f"Graph overview error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Overview failed: {str(e)[:200]}")


@router.get("/health")
async def semantic_search_health():
    """Check if Vector Search index is available"""
    try:
        import os
        from databricks.vector_search.client import VectorSearchClient
        vsc = VectorSearchClient(
            workspace_url=config.DATABRICKS_WORKSPACE_URL,
            service_principal_client_id=os.getenv("DATABRICKS_CLIENT_ID"),
            service_principal_client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
        )
        index = vsc.get_index(VS_ENDPOINT_NAME, VS_INDEX_NAME)
        status = index.describe().get("status", {})
        return {
            "status": "ok",
            "index": VS_INDEX_NAME,
            "ready": status.get("ready", False),
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}
