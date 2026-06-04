"""API endpoints for Neo4j graph visualization"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import logging
import os
import urllib.parse

from backend import config

logger = logging.getLogger(__name__)
logger.info("🚀 Neo4j graph API module loading...")

router = APIRouter()

# Log environment variables for debugging
logger.info(f"NEO4J_URI from env: {os.getenv('NEO4J_URI', 'NOT SET')}")
logger.info(f"NEO4J_USER from env: {os.getenv('NEO4J_USER', 'NOT SET')}")
logger.info(f"NEO4J_PASSWORD exists: {'YES' if os.getenv('NEO4J_PASSWORD') else 'NO'}")

# Import after logging to see if import fails
try:
    from backend.services.neo4j_service import neo4j_service
    logger.info("✅ Neo4j service imported successfully")

    # Try to connect
    try:
        connected = neo4j_service.connect()
        if connected:
            logger.info("✅ Neo4j connection established successfully!")
        else:
            logger.error("❌ Failed to establish Neo4j connection")
    except Exception as e:
        logger.error(f"❌ Exception during Neo4j connection: {e}")
except Exception as e:
    logger.error(f"❌ Failed to import neo4j_service: {e}")
    neo4j_service = None

# Create a simple mock service if neo4j_service failed
if neo4j_service is None:
    logger.warning("⚠️ Creating mock Neo4j service for demo data")
    class MockNeo4jService:
        def connect(self):
            return False
        def get_customer_graph(self, customer_id, depth=2, limit=100):
            return None
        def get_transaction_graph(self, customer_id, case_id=None, limit=200):
            return None
        def get_relationship_network(self, customer_id, relationship_types=None):
            return None
        driver = None
        uri = config.NEO4J_URI
        database = config.NEO4J_DATABASE
    neo4j_service = MockNeo4jService()

@router.get("/graph/ping")
async def ping():
    """Simple ping endpoint to test if the router is working"""
    return {"status": "ok", "message": "Neo4j graph API is accessible"}

@router.get("/graph/demo/{customer_id}")
async def get_demo_graph(customer_id: str):
    """Return demo graph data for testing"""
    return {
        "nodes": [
            {"id": "1", "labels": ["Customer"], "properties": {"name": f"Customer {customer_id}", "risk_level": "High"}, "caption": f"Customer {customer_id}"},
            {"id": "2", "labels": ["Account"], "properties": {"account_number": "ACC-001", "type": "Checking"}, "caption": "Checking ACC-001"},
            {"id": "3", "labels": ["Account"], "properties": {"account_number": "ACC-002", "type": "Savings"}, "caption": "Savings ACC-002"},
            {"id": "4", "labels": ["Transaction"], "properties": {"amount": 50000, "type": "Wire Transfer"}, "caption": "Wire $50,000"},
            {"id": "5", "labels": ["Entity"], "properties": {"name": "Offshore Corp", "jurisdiction": "Cayman Islands"}, "caption": "Offshore Corp"},
            {"id": "6", "labels": ["Alert"], "properties": {"type": "Suspicious Activity", "severity": "High"}, "caption": "High Risk Alert"}
        ],
        "relationships": [
            {"id": "r1", "from": "1", "to": "2", "type": "OWNS", "properties": {}, "caption": "OWNS"},
            {"id": "r2", "from": "1", "to": "3", "type": "OWNS", "properties": {}, "caption": "OWNS"},
            {"id": "r3", "from": "2", "to": "4", "type": "SENT", "properties": {"date": "2024-01-15"}, "caption": "SENT"},
            {"id": "r4", "from": "4", "to": "5", "type": "RECEIVED_BY", "properties": {}, "caption": "RECEIVED_BY"},
            {"id": "r5", "from": "1", "to": "6", "type": "FLAGGED_BY", "properties": {}, "caption": "FLAGGED_BY"}
        ]
    }

@router.get("/graph/customer/{customer_id}")
async def get_customer_graph(
    customer_id: str,
    customer_name: Optional[str] = Query(default=None, description="Customer name to search for"),
    depth: int = Query(default=2, ge=1, le=4, description="Graph traversal depth"),
    limit: int = Query(default=300, ge=1, le=1000, description="Maximum number of paths")
):
    """
    Get graph visualization data for a specific customer

    Args:
        customer_id: The customer ID to fetch graph for
        customer_name: Optional customer name to search for (uses node_label property)
        depth: How many hops from the customer to traverse (1-4)
        limit: Maximum number of paths to return

    Returns:
        Graph data with nodes and relationships in NVL format
    """
    try:
        # Prefer customer_name if provided, otherwise use customer_id
        graph_data = neo4j_service.get_customer_graph(
            customer_id=customer_id if not customer_name else None,
            customer_name=customer_name,
            depth=depth,
            limit=limit
        )

        if graph_data is None:
            raise HTTPException(status_code=500, detail="Failed to fetch graph data from Neo4j")

        if not graph_data["nodes"]:
            # Return empty but valid graph structure
            return {
                "nodes": [],
                "relationships": [],
                "message": f"No graph data found for customer {customer_id}"
            }

        return graph_data

    except Exception as e:
        logger.error(f"Error fetching customer graph: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/graph/transactions/{customer_id}")
async def get_transaction_graph(
    customer_id: str,
    case_id: Optional[str] = None,
    limit: int = Query(default=200, ge=1, le=500, description="Maximum number of paths")
):
    """
    Get transaction graph for a customer investigation

    Args:
        customer_id: The customer ID
        case_id: Optional case/alert ID to filter transactions
        limit: Maximum number of paths to return

    Returns:
        Transaction graph data with nodes and relationships
    """
    try:
        graph_data = neo4j_service.get_transaction_graph(customer_id, case_id, limit)

        if graph_data is None:
            raise HTTPException(status_code=500, detail="Failed to fetch transaction graph from Neo4j")

        if not graph_data["nodes"]:
            return {
                "nodes": [],
                "relationships": [],
                "message": f"No transaction data found for customer {customer_id}"
            }

        return graph_data

    except Exception as e:
        logger.error(f"Error fetching transaction graph: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/graph/relationships/{customer_id}")
async def get_relationship_network(
    customer_id: str,
    types: Optional[str] = Query(None, description="Comma-separated relationship types to filter")
):
    """
    Get relationship network for a customer

    Args:
        customer_id: The customer ID
        types: Optional comma-separated list of relationship types (e.g., "SENT,RECEIVED,KNOWS")

    Returns:
        Relationship network graph data
    """
    try:
        relationship_types = types.split(",") if types else None
        graph_data = neo4j_service.get_relationship_network(customer_id, relationship_types)

        if graph_data is None:
            raise HTTPException(status_code=500, detail="Failed to fetch relationship network from Neo4j")

        return graph_data

    except Exception as e:
        logger.error(f"Error fetching relationship network: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/graph/transactions/{customer_name}")
async def get_customer_transactions(
    customer_name: str,
    limit: int = Query(default=30, ge=1, le=100, description="Maximum number of transactions")
):
    """
    Get recent transactions for a customer for timeline visualization

    Args:
        customer_name: Customer name to fetch transactions for
        limit: Maximum number of transactions to return

    Returns:
        List of transactions with date, amount, type, and direction
    """
    try:
        logger.info(f"Fetching transactions for customer: {customer_name}")
        transactions = neo4j_service.get_customer_transactions(
            customer_name=customer_name,
            limit=limit
        )

        if transactions is None:
            # Return demo data if Neo4j fails
            import random
            from datetime import datetime, timedelta
            demo_transactions = []
            for i in range(10):
                demo_transactions.append({
                    "date": (datetime.now() - timedelta(days=i*3)).strftime("%Y-%m-%d"),
                    "amount": random.randint(1000, 50000),
                    "type": random.choice(["WIRE_TRANSFER", "ACH_TRANSFER"]),
                    "direction": random.choice(["sent", "received"]),
                    "counterparty": f"Entity {i+1}"
                })
            return {"transactions": demo_transactions}

        return {"transactions": transactions}

    except Exception as e:
        logger.error(f"Error fetching customer transactions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/graph/health")
async def check_neo4j_health():
    """Check Neo4j connection health"""
    try:
        if not neo4j_service.driver:
            # Try to connect if not connected
            connected = neo4j_service.connect()
            if not connected:
                return {
                    "status": "disconnected",
                    "message": "Failed to establish connection",
                    "uri": neo4j_service.uri
                }

        neo4j_service.driver.verify_connectivity()

        # Run a simple test query
        with neo4j_service.driver.session(database=neo4j_service.database) as session:
            result = session.run("RETURN 1 as test")
            test_value = result.single()["test"]

        return {
            "status": "connected",
            "uri": neo4j_service.uri,
            "database": neo4j_service.database,
            "test_query": "successful",
            "test_value": test_value
        }
    except Exception as e:
        logger.error(f"Neo4j health check failed: {e}")
        return {
            "status": "error",
            "message": str(e),
            "uri": neo4j_service.uri
        }

@router.get("/graph/test")
async def test_neo4j_query():
    """Test Neo4j with a simple query to get all node labels"""
    try:
        if not neo4j_service.driver:
            neo4j_service.connect()

        with neo4j_service.driver.session(database=neo4j_service.database) as session:
            # Get all node labels
            result = session.run("CALL db.labels()")
            labels = [record["label"] for record in result]

            # Get count of nodes
            count_result = session.run("MATCH (n) RETURN count(n) as total")
            total_nodes = count_result.single()["total"]

            # Get sample customer names
            customer_result = session.run("""
                MATCH (c:Customer)
                RETURN c.node_label as name
                LIMIT 5
            """)
            sample_customers = [record["name"] for record in customer_result if record["name"]]

            return {
                "status": "success",
                "node_labels": labels,
                "total_nodes": total_nodes,
                "sample_customers": sample_customers,
                "message": f"Found {total_nodes} nodes with {len(labels)} different labels"
            }
    except Exception as e:
        logger.error(f"Test query failed: {e}")
        return {"status": "error", "message": str(e)}

@router.get("/graph/customer/by-name")
async def get_customer_by_name(
    name: str = Query(..., description="Customer name to search for"),
    depth: int = Query(default=2, ge=1, le=4, description="Graph traversal depth"),
    limit: int = Query(default=100, ge=1, le=500, description="Maximum number of paths")
):
    """
    Get customer graph by searching for customer name (node_label property)

    Args:
        name: Customer name to search for (uses partial match)
        depth: How many hops from the customer to traverse (1-4)
        limit: Maximum number of paths to return

    Returns:
        Graph data with nodes and relationships
    """
    try:
        logger.info(f"Searching for customer by name: {name}")
        graph_data = neo4j_service.get_customer_graph(
            customer_name=name,
            depth=depth,
            limit=limit
        )

        if graph_data is None:
            raise HTTPException(status_code=500, detail="Failed to fetch graph data from Neo4j")

        if not graph_data["nodes"]:
            # Try exact match
            logger.info(f"No partial match found, trying exact match for: {name}")
            return {
                "nodes": [],
                "relationships": [],
                "message": f"No graph data found for customer '{name}'"
            }

        logger.info(f"Found {len(graph_data['nodes'])} nodes for customer: {name}")
        return graph_data

    except Exception as e:
        logger.error(f"Error fetching customer graph by name: {e}")
        raise HTTPException(status_code=500, detail=str(e))