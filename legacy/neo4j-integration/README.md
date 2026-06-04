# Neo4j Graph Backend (Reference Implementation)

This directory contains the original Neo4j Aura integration that powered
the SherlockAML graph visualization before it was migrated to native
Databricks graph tables (`graph_nodes` / `graph_edges`).

It is **not** part of the active build. It lives here as a working
reference for teams who would prefer Neo4j as their graph backend, or
who want to see how the same surface area can be implemented against a
labeled-property graph rather than relational tables.

## Files

| File | Purpose |
|---|---|
| `neo4j_service.py` | Async Neo4j Aura connection service. Connects via `neo4j+s://` URI, executes Cypher queries, converts traversal paths to Cytoscape-formatted nodes and relationships. |
| `neo4j_graph.py` | FastAPI router exposing endpoints equivalent to the current `databricks_graph.py`: `/graph/customer/{id}`, `/graph/transactions/{id}`, `/graph/network/{id}`, plus health checks. |

## When to choose Neo4j over Databricks graph tables

The active implementation uses two Delta tables (`graph_nodes` and
`graph_edges`) read via the SQL warehouse or optionally Lakebase
Postgres. That stack is the right call for most Databricks-native AML
deployments: data is governed by Unity Catalog, queries reuse existing
compute, and there is no separate graph database to operate.

The Neo4j reference here is more compelling if your team:
- Already operates a Neo4j Aura instance and wants AML on top of it.
- Needs deep, variable-depth path queries (4+ hops) where Cypher's
  pattern matching outperforms the recursive-CTE / window-function
  approach used in this repo's SQL.
- Wants to extend the graph schema rapidly with arbitrary properties
  on nodes and edges (Neo4j handles schemaless property bags more
  fluidly than relational columns).

## How to integrate (high level)

To swap this in for the Databricks graph backend:

1. **Restore the files into the app bundle:**
   ```bash
   cp legacy/neo4j-integration/neo4j_service.py fins-aml-app-bundle/backend/services/
   cp legacy/neo4j-integration/neo4j_graph.py   fins-aml-app-bundle/backend/api/
   ```

2. **Wire the router in `fins-aml-app-bundle/main.py`** (replacing the
   `databricks_graph` import):
   ```python
   from backend.api.neo4j_graph import router as graph_router
   app.include_router(graph_router, prefix="/api/neo4j-graph", tags=["graph"])
   ```

3. **Update the frontend** (`fins-aml-app-bundle/frontend/build/index.html`)
   to call the Neo4j endpoint URLs:
   ```
   /api/databricks-graph/graph/customer/... → /api/neo4j-graph/graph/customer/...
   ```

4. **Configure connection environment variables** (used by
   `neo4j_service.py`). Add to `app.yaml`:
   ```yaml
   - name: 'NEO4J_URI'
     value: '${var.neo4j_uri}'        # e.g. neo4j+s://xxxxx.databases.neo4j.io
   - name: 'NEO4J_USERNAME'
     value: '${var.neo4j_username}'
   - name: 'NEO4J_PASSWORD'
     valueFrom: <secret-name>          # store as a Databricks secret
   ```

5. **Add the driver** to `requirements.txt`:
   ```
   neo4j>=5.14.0
   ```

6. **Load your graph into Neo4j.** The schema used by the original
   integration:
   - Nodes labeled `Customer`, `Account`, `Counterparty`, `Alert`,
     `Watchlist`, `Transaction`
   - Relationships: `OWNS_ACCOUNT`, `WIRE_TRANSFER`, `HAS_ALERT`,
     `MATCHES_WATCHLIST`, etc.
   - Properties mirror the columns in `graph_nodes` / `graph_edges`
     (node_id, node_label, risk_score, etc.).

   A `LOAD CSV` ingest from the same Delta tables is the simplest path.

## Status

Frozen as-of the 2026-04 migration commit. Not maintained in sync with
the active app. Treat as a reference, not a drop-in module.
