"""API endpoints for Databricks-native graph visualization"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, Dict, List, Any
import logging
import asyncio

logger = logging.getLogger(__name__)
logger.info("🚀 Databricks graph API module loading...")

router = APIRouter()

# Import database service
from backend.services.database import DatabaseService
from backend import config
from backend.services import lakebase as lakebase_service

# Dependency to get database service
async def get_db_service() -> DatabaseService:
    from main import db_service
    if not db_service:
        raise HTTPException(status_code=503, detail="Database service unavailable")
    return db_service


async def _execute_graph_query(sql: str, db_service: DatabaseService) -> List[Dict[str, Any]]:
    """Backwards-compatible alias for the shared router in lakebase_service."""
    return await lakebase_service.route_query(db_service, sql)

def format_node_for_cytoscape(node: Dict[str, Any]) -> Dict[str, Any]:
    """Format a Databricks graph node for Cytoscape visualization"""
    import json

    # Parse properties if it's a JSON string
    properties = node.get('properties', {})
    if isinstance(properties, str):
        try:
            properties = json.loads(properties)
        except:
            properties = {}

    # Determine node color based on type and risk
    node_type = str(node.get('node_type', 'unknown'))

    # Ensure risk_score is a number - handle all possible types
    raw_risk = node.get('risk_score', 0)
    if raw_risk is None:
        risk_score = 0
    elif isinstance(raw_risk, (int, float)):
        risk_score = int(raw_risk)
    elif isinstance(raw_risk, str):
        try:
            risk_score = int(float(raw_risk)) if raw_risk else 0
        except (ValueError, TypeError):
            risk_score = 0
    else:
        risk_score = 0

    # Determine color based on node type (aligned with app palette)
    if node_type == 'customer':
        color = '#1D4ED8'       # Primary Blue — always blue regardless of risk
    elif node_type == 'account':
        color = '#7FB3D5'       # Sky Blue
    elif node_type == 'counterparty':
        color = '#D4AF37'       # Rich Gold
    elif node_type == 'watchlist':
        color = '#730D21'       # Dark Red
    elif node_type == 'alert':
        color = '#BD2B26'       # Red
    elif node_type == 'transaction':
        color = '#64748B'       # Grey
    else:
        color = '#64748B'

    return {
        "id": f"{node_type}_{node.get('node_id', '')}",
        "labels": [node_type.upper()],
        "type": node_type,  # Add type for frontend to use
        "properties": {
            "name": node.get('node_label', ''),
            "risk_score": risk_score,
            "risk_category": node.get('risk_category', 'unknown'),
            **properties
        },
        "caption": node.get('node_label', ''),
        "color": color
        # Size is now determined by frontend based on node type
    }

def format_edge_for_cytoscape(edge: Dict[str, Any], edge_id: int) -> Dict[str, Any]:
    """Format a Databricks graph edge for Cytoscape visualization"""
    import json

    # Parse properties if it's a JSON string
    properties = edge.get('properties', {})
    if isinstance(properties, str):
        try:
            properties = json.loads(properties)
        except:
            properties = {}

    return {
        "id": f"r{edge_id}",
        "from": f"{edge.get('source_node_type', '')}_{edge.get('source_node_id', '')}",
        "to": f"{edge.get('target_node_type', '')}_{edge.get('target_node_id', '')}",
        "type": edge.get('edge_type', 'RELATED'),
        "properties": properties,
        "caption": edge.get('edge_type', ''),
        "weight": edge.get('weight', 1.0)
    }

@router.get("/graph/ping")
async def ping():
    """Simple ping endpoint to test if the router is working"""
    return {"status": "ok", "message": "Databricks graph API is accessible", "source": "databricks"}

@router.get("/graph/health")
async def check_databricks_graph_health(db_service: DatabaseService = Depends(get_db_service)):
    """Check Databricks graph data availability"""
    try:
        # Check if graph tables exist
        from backend import config
        check_query = f"""
        SELECT
            (SELECT COUNT(*) FROM {config.CATALOG}.{config.SCHEMA}.graph_nodes LIMIT 1) as node_count,
            (SELECT COUNT(*) FROM {config.CATALOG}.{config.SCHEMA}.graph_edges LIMIT 1) as edge_count
        """

        result = await db_service.execute_query(check_query)

        if result and len(result) > 0:
            stats = result[0]
            return {
                "status": "connected",
                "source": "databricks",
                "statistics": {
                    "total_nodes": stats.get('node_count', 0),
                    "total_edges": stats.get('edge_count', 0)
                },
                "message": "Databricks graph data is available"
            }
        else:
            return {
                "status": "error",
                "message": "Graph tables exist but contain no data"
            }

    except Exception as e:
        logger.error(f"Databricks graph health check failed: {e}")
        return {
            "status": "error",
            "message": str(e),
            "source": "databricks"
        }

@router.get("/graph/demo/{customer_id}")
async def get_demo_graph(customer_id: str):
    """Return demo graph data for testing"""
    return {
        "nodes": [
            {"id": "customer_1", "labels": ["Customer"], "properties": {"name": f"Customer {customer_id}", "risk_level": "High"}, "caption": f"Customer {customer_id}"},
            {"id": "account_1", "labels": ["Account"], "properties": {"account_number": "ACC-001", "type": "Checking"}, "caption": "Checking ACC-001"},
            {"id": "account_2", "labels": ["Account"], "properties": {"account_number": "ACC-002", "type": "Savings"}, "caption": "Savings ACC-002"},
            {"id": "transaction_1", "labels": ["Transaction"], "properties": {"amount": 50000, "type": "Wire Transfer"}, "caption": "Wire $50,000"},
            {"id": "counterparty_1", "labels": ["Entity"], "properties": {"name": "Offshore Corp", "jurisdiction": "Cayman Islands"}, "caption": "Offshore Corp"},
            {"id": "alert_1", "labels": ["Alert"], "properties": {"type": "Suspicious Activity", "severity": "High"}, "caption": "High Risk Alert"}
        ],
        "relationships": [
            {"id": "r1", "from": "customer_1", "to": "account_1", "type": "OWNS", "properties": {}, "caption": "OWNS"},
            {"id": "r2", "from": "customer_1", "to": "account_2", "type": "OWNS", "properties": {}, "caption": "OWNS"},
            {"id": "r3", "from": "account_1", "to": "transaction_1", "type": "SENT", "properties": {"date": "2024-01-15"}, "caption": "SENT"},
            {"id": "r4", "from": "transaction_1", "to": "counterparty_1", "type": "RECEIVED_BY", "properties": {}, "caption": "RECEIVED_BY"},
            {"id": "r5", "from": "customer_1", "to": "alert_1", "type": "FLAGGED_BY", "properties": {}, "caption": "FLAGGED_BY"}
        ],
        "source": "demo"
    }

@router.get("/graph/customer/{customer_id}")
async def get_customer_graph(
    customer_id: str,
    customer_name: Optional[str] = Query(default=None, description="Customer name to search for"),
    depth: int = Query(default=2, ge=1, le=4, description="Graph traversal depth"),
    limit: int = Query(default=100, ge=1, le=500, description="Maximum number of edges"),
    db_service: DatabaseService = Depends(get_db_service)
):
    """
    Get graph visualization data directly from Databricks tables

    This replaces the Neo4j dependency by querying the graph_nodes and graph_edges
    tables that are already created in your Databricks catalog.
    """
    try:
        from backend import config
        # Build the query to fetch customer and related nodes
        if customer_name:
            # Search by customer name
            base_condition = f"LOWER(node_label) LIKE LOWER('%{customer_name}%')"
        else:
            # Search by customer ID
            base_condition = f"node_id = '{customer_id}'"

        # Get the target customer
        customer_query = f"""
        SELECT node_id, node_type, node_label, risk_score, risk_category, properties
        FROM {config.CATALOG}.{config.SCHEMA}.graph_nodes
        WHERE node_type = 'customer' AND {base_condition}
        LIMIT 1
        """

        customer_result = await _execute_graph_query(customer_query, db_service)

        if not customer_result:
            return {
                "nodes": [],
                "relationships": [],
                "message": f"No customer found for {'name: ' + customer_name if customer_name else 'ID: ' + customer_id}"
            }

        target_customer = customer_result[0]
        target_id = target_customer['node_id']

        # Get all edges connected to this customer, deduplicated.
        # The graph_edges table may contain both directions (A->B and B->A)
        # for the same relationship. We use LEAST/GREATEST to normalize the
        # pair and pick one representative row per unique edge.
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
            WHERE (source_node_id = '{target_id}' AND source_node_type = 'customer')
               OR (target_node_id = '{target_id}' AND target_node_type = 'customer')
        )
        SELECT * FROM ranked WHERE rn = 1
        ORDER BY weight DESC
        LIMIT {limit}
        """

        edges_result = await _execute_graph_query(edges_query, db_service)

        # Collect all node IDs we need
        node_ids = {('customer', str(target_id))}
        for edge in edges_result:
            node_ids.add((edge['source_node_type'], str(edge['source_node_id'])))
            node_ids.add((edge['target_node_type'], str(edge['target_node_id'])))

        # Get all nodes involved
        nodes_conditions = " OR ".join([
            f"(node_type = '{node_type}' AND node_id = '{node_id}')"
            for node_type, node_id in node_ids
        ])

        nodes_query = f"""
        SELECT DISTINCT node_id, node_type, node_label, risk_score, risk_category, properties
        FROM {config.CATALOG}.{config.SCHEMA}.graph_nodes
        WHERE {nodes_conditions}
        """

        nodes_result = await _execute_graph_query(nodes_query, db_service)

        # Format results for Cytoscape
        nodes = [format_node_for_cytoscape(node) for node in nodes_result]
        relationships = [format_edge_for_cytoscape(edge, idx) for idx, edge in enumerate(edges_result)]

        # Debug logging
        logger.info(f"Query returned {len(nodes_result)} nodes and {len(edges_result)} edges")
        if edges_result:
            logger.info(f"Sample edge: {edges_result[0]}")
        if relationships:
            logger.info(f"Sample formatted relationship: {relationships[0]}")

        # For depth > 1, get second-degree connections
        # Ensure limit is an integer for comparison
        limit_int = int(limit) if not isinstance(limit, int) else limit
        if depth > 1 and len(relationships) < limit_int:
            # Get nodes connected to our first-degree nodes
            first_degree_node_ids = set()
            for edge in edges_result:
                if str(edge['source_node_id']) != str(target_id):
                    first_degree_node_ids.add((edge['source_node_type'], str(edge['source_node_id'])))
                if str(edge['target_node_id']) != str(target_id):
                    first_degree_node_ids.add((edge['target_node_type'], str(edge['target_node_id'])))

            if first_degree_node_ids:
                # Build conditions for second-degree edges
                second_degree_conditions = " OR ".join([
                    f"(source_node_type = '{node_type}' AND source_node_id = '{node_id}')"
                    for node_type, node_id in first_degree_node_ids
                ] + [
                    f"(target_node_type = '{node_type}' AND target_node_id = '{node_id}')"
                    for node_type, node_id in first_degree_node_ids
                ])

                second_edges_query = f"""
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
                    WHERE ({second_degree_conditions})
                )
                SELECT * FROM ranked WHERE rn = 1
                LIMIT {limit_int - len(relationships)}
                """

                second_edges_result = await _execute_graph_query(second_edges_query, db_service)

                # Add new edges, deduplicating against already-fetched edges
                seen_edges = {(r['from'], r['to'], r['type']) for r in relationships}
                new_node_ids = set()
                for idx, edge in enumerate(second_edges_result):
                    formatted = format_edge_for_cytoscape(edge, len(relationships) + idx)
                    edge_key = (formatted['from'], formatted['to'], formatted['type'])
                    reverse_key = (formatted['to'], formatted['from'], formatted['type'])
                    if edge_key not in seen_edges and reverse_key not in seen_edges:
                        relationships.append(formatted)
                        seen_edges.add(edge_key)
                    new_node_ids.add((edge['source_node_type'], str(edge['source_node_id'])))
                    new_node_ids.add((edge['target_node_type'], str(edge['target_node_id'])))

                # Remove already fetched nodes
                new_node_ids = new_node_ids - node_ids

                if new_node_ids:
                    # Get additional nodes
                    new_nodes_conditions = " OR ".join([
                        f"(node_type = '{node_type}' AND node_id = '{node_id}')"
                        for node_type, node_id in new_node_ids
                    ])

                    new_nodes_query = f"""
                    SELECT DISTINCT node_id, node_type, node_label, risk_score, risk_category, properties
                    FROM {config.CATALOG}.{config.SCHEMA}.graph_nodes
                    WHERE {new_nodes_conditions}
                    """

                    new_nodes_result = await _execute_graph_query(new_nodes_query, db_service)
                    for node in new_nodes_result:
                        nodes.append(format_node_for_cytoscape(node))

        return {
            "nodes": nodes,
            "relationships": relationships,
            "source": "databricks",
            "message": f"Found {len(nodes)} nodes and {len(relationships)} relationships (depth={depth})"
        }

    except Exception as e:
        import traceback
        logger.error(f"Error fetching graph from Databricks: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        logger.error(f"Query parameters - customer_id: {customer_id}, customer_name: {customer_name}, depth: {depth}, limit: {limit}, limit type: {type(limit)}")
        # Return demo data as fallback
        return await get_demo_graph(customer_id)

@router.get("/graph/customer/by-name")
async def get_customer_by_name(
    name: str = Query(..., description="Customer name to search for"),
    depth: int = Query(default=2, ge=1, le=4, description="Graph traversal depth"),
    limit: int = Query(default=100, ge=1, le=500, description="Maximum number of edges")
):
    """Get customer graph by searching for customer name"""
    return await get_customer_graph(
        customer_id="",
        customer_name=name,
        depth=depth,
        limit=limit
    )

@router.get("/graph/test")
async def test_databricks_query(db_service: DatabaseService = Depends(get_db_service)):
    """Test Databricks connection with graph data"""
    try:
        from backend import config
        # Get summary statistics
        test_query = f"""
        SELECT
            'nodes' as data_type,
            node_type,
            COUNT(*) as count,
            ROUND(AVG(risk_score), 1) as avg_risk
        FROM {config.CATALOG}.{config.SCHEMA}.graph_nodes
        GROUP BY node_type

        UNION ALL

        SELECT
            'edges' as data_type,
            edge_type as node_type,
            COUNT(*) as count,
            ROUND(AVG(weight) * 100, 1) as avg_risk
        FROM {config.CATALOG}.{config.SCHEMA}.graph_edges
        GROUP BY edge_type
        """

        result = await db_service.execute_query(test_query)

        # Get sample customers
        customers_query = f"""
        SELECT node_id, node_label
        FROM {config.CATALOG}.{config.SCHEMA}.graph_nodes
        WHERE node_type = 'customer'
        LIMIT 5
        """

        customers = await db_service.execute_query(customers_query)

        return {
            "status": "success",
            "statistics": result,
            "sample_customers": [
                {"id": c['node_id'], "name": c['node_label']} for c in customers
            ],
            "message": "Databricks graph connection successful"
        }

    except Exception as e:
        logger.error(f"Test query failed: {e}")
        return {"status": "error", "message": str(e)}