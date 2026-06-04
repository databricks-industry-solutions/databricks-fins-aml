"""Neo4j Database Service for Graph Visualization"""

import os
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)
logger.info("🔧 Neo4j service module loading...")

try:
    from neo4j import GraphDatabase
    from neo4j.exceptions import Neo4jError
    logger.info("✅ Neo4j driver imported successfully")
except ImportError as e:
    logger.error(f"❌ Failed to import neo4j driver: {e}")
    logger.error("Make sure 'neo4j' is installed: pip install neo4j")
    raise

class Neo4jService:
    """Service for interacting with Neo4j Aura graph database"""

    def __init__(self):
        from backend import config
        self.uri = config.NEO4J_URI
        self.user = config.NEO4J_USER
        self.password = config.NEO4J_PASSWORD
        self.database = config.NEO4J_DATABASE
        self.driver = None

        if not self.password:
            raise ValueError("NEO4J_PASSWORD environment variable is required")

    def connect(self):
        """Initialize connection to Neo4j"""
        try:
            logger.info(f"Attempting to connect to Neo4j at {self.uri}")
            logger.info(f"Using database: {self.database}, user: {self.user}")

            self.driver = GraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password)
            )

            # Verify connectivity
            self.driver.verify_connectivity()
            logger.info(f"✅ Successfully connected to Neo4j at {self.uri}")

            # Test query
            with self.driver.session(database=self.database) as session:
                result = session.run("RETURN 'Connection test successful' as message")
                message = result.single()["message"]
                logger.info(f"✅ Neo4j test query result: {message}")

            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Neo4j: {e}")
            logger.error(f"URI: {self.uri}, Database: {self.database}, User: {self.user}")
            return False

    def close(self):
        """Close Neo4j connection"""
        if self.driver:
            self.driver.close()

    def path_to_graph_format(self, paths: List[Any]) -> Dict[str, List[Dict]]:
        """Convert Neo4j paths to NVL-compatible graph format"""
        nodes = {}
        relationships = {}

        for path in paths:
            # Extract nodes
            for node in path.nodes:
                node_id = str(node.id)
                if node_id not in nodes:
                    properties = dict(node)
                    # Use node_label for caption if available, otherwise fall back to other properties
                    caption = (properties.get("node_label") or
                              properties.get("name") or
                              properties.get("customer_name") or
                              properties.get("uid") or
                              f"Node {node_id}")
                    nodes[node_id] = {
                        "id": node_id,
                        "labels": list(node.labels),
                        "properties": properties,
                        "caption": caption
                    }

            # Extract relationships
            for rel in path.relationships:
                rel_id = str(rel.id)
                if rel_id not in relationships:
                    relationships[rel_id] = {
                        "id": rel_id,
                        "from": str(rel.start_node.id),
                        "to": str(rel.end_node.id),
                        "type": rel.type,
                        "properties": dict(rel),
                        "caption": rel.type
                    }

        return {
            "nodes": list(nodes.values()),
            "relationships": list(relationships.values())
        }

    def get_customer_graph(
        self,
        customer_id: str = None,
        customer_name: str = None,
        depth: int = 2,
        limit: int = 300
    ) -> Optional[Dict[str, List[Dict]]]:
        """Get graph data for a specific customer by ID or name"""
        if not self.driver:
            if not self.connect():
                return None

        try:
            paths = []

            # If we have a customer name, search by name (node_label property)
            if customer_name:
                logger.info(f"Searching for customer by name: {customer_name}")

                # Simple query that works - limit results rather than complex aggregation
                cypher = f"""
                MATCH (c:Customer)
                WHERE lower(c.node_label) CONTAINS lower($customer_name)
                WITH c LIMIT 1
                MATCH p=(c)-[*1..{min(depth, 3)}]-(n)
                RETURN p
                LIMIT $limit
                """

                with self.driver.session(database=self.database) as session:
                    result = session.run(
                        cypher,
                        customer_name=customer_name,
                        limit=limit
                    )
                    paths = [record["p"] for record in result]

                    if paths:
                        logger.info(f"Found {len(paths)} paths for customer: {customer_name}")
                    else:
                        logger.warning(f"No paths found for customer name: {customer_name}")

            # Fall back to ID-based search if no name provided or no results
            elif customer_id:
                logger.info(f"Searching for customer by ID: {customer_id}")

                # Simple query based on depth
                cypher = f"""
                MATCH p=(c:Customer)-[*1..{min(depth, 3)}]-(n)
                WHERE c.customer_id = $customer_id OR c.id = $customer_id OR c.uid = $customer_id
                RETURN p
                LIMIT $limit
                """

                with self.driver.session(database=self.database) as session:
                    result = session.run(
                        cypher,
                        customer_id=customer_id,
                        limit=limit
                    )
                    paths = [record["p"] for record in result]
            else:
                logger.error("No customer_id or customer_name provided")
                return None

            return self.path_to_graph_format(paths)

        except Neo4jError as e:
            logger.error(f"Neo4j query error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching customer graph: {e}")
            return None

    def get_transaction_graph(
        self,
        customer_id: str,
        case_id: Optional[str] = None,
        limit: int = 200
    ) -> Optional[Dict[str, List[Dict]]]:
        """Get transaction graph for investigation"""
        if not self.driver:
            if not self.connect():
                return None

        try:
            if case_id:
                # Query for specific case
                cypher = """
                MATCH (c:Customer {customer_id: $customer_id})
                MATCH p=(c)-[:SENT|RECEIVED]-(t:Transaction)-[:SENT|RECEIVED]-(other)
                WHERE t.case_id = $case_id OR t.alert_id = $case_id
                RETURN p
                LIMIT $limit
                """
                params = {
                    "customer_id": customer_id,
                    "case_id": case_id,
                    "limit": limit
                }
            else:
                # Query for all customer transactions
                cypher = """
                MATCH (c:Customer {customer_id: $customer_id})
                MATCH p=(c)-[:SENT|RECEIVED]-(t:Transaction)-[:SENT|RECEIVED]-(other)
                RETURN p
                LIMIT $limit
                """
                params = {
                    "customer_id": customer_id,
                    "limit": limit
                }

            with self.driver.session(database=self.database) as session:
                result = session.run(cypher, **params)
                paths = [record["p"] for record in result]

                if not paths:
                    # Fallback to simpler query
                    cypher_simple = """
                    MATCH p=(c:Customer)-[*1..3]-(n)
                    WHERE c.customer_id = $customer_id OR c.id = $customer_id
                    RETURN p
                    LIMIT $limit
                    """
                    result = session.run(
                        cypher_simple,
                        customer_id=customer_id,
                        limit=limit
                    )
                    paths = [record["p"] for record in result]

                return self.path_to_graph_format(paths)

        except Neo4jError as e:
            logger.error(f"Neo4j query error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching transaction graph: {e}")
            return None

    def get_customer_transactions(
        self,
        customer_name: str,
        limit: int = 30
    ) -> Optional[List[Dict]]:
        """Get recent transactions for timeline chart"""
        if not self.driver:
            if not self.connect():
                return None

        try:
            cypher = """
            MATCH (c:Customer)
            WHERE lower(c.node_label) CONTAINS lower($customer_name)
            WITH c LIMIT 1
            MATCH (c)-[r:SENDS_TRANSFER|RECEIVES_TRANSFER|ACH_TRANSFER|WIRE_TRANSFER]-(counterparty)
            WHERE r.amount IS NOT NULL AND r.transaction_date IS NOT NULL
            RETURN
                r.transaction_date as date,
                r.amount as amount,
                type(r) as type,
                CASE
                    WHEN type(r) IN ['SENDS_TRANSFER', 'ACH_TRANSFER', 'WIRE_TRANSFER'] AND startNode(r) = c THEN 'sent'
                    ELSE 'received'
                END as direction,
                counterparty.node_label as counterparty
            ORDER BY r.transaction_date DESC
            LIMIT $limit
            """

            with self.driver.session(database=self.database) as session:
                result = session.run(cypher, customer_name=customer_name, limit=limit)

                transactions = []
                for record in result:
                    transactions.append({
                        "date": str(record["date"]),
                        "amount": float(record["amount"]) if record["amount"] else 0,
                        "type": record["type"],
                        "direction": record["direction"],
                        "counterparty": record["counterparty"] or "Unknown"
                    })

                logger.info(f"Found {len(transactions)} transactions for customer: {customer_name}")
                return transactions

        except Neo4jError as e:
            logger.error(f"Neo4j query error getting transactions: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching transactions: {e}")
            return None

    def get_relationship_network(
        self,
        customer_id: str,
        relationship_types: List[str] = None
    ) -> Optional[Dict[str, List[Dict]]]:
        """Get relationship network for a customer"""
        if not self.driver:
            if not self.connect():
                return None

        try:
            if relationship_types:
                rel_filter = "|".join(relationship_types)
                cypher = f"""
                MATCH p=(c:Customer {{customer_id: $customer_id}})-[:{rel_filter}*1..3]-(n)
                RETURN p
                LIMIT 500
                """
            else:
                cypher = """
                MATCH p=(c:Customer {customer_id: $customer_id})-[*1..3]-(n)
                RETURN p
                LIMIT 500
                """

            with self.driver.session(database=self.database) as session:
                result = session.run(cypher, customer_id=customer_id)
                paths = [record["p"] for record in result]
                return self.path_to_graph_format(paths)

        except Neo4jError as e:
            logger.error(f"Neo4j query error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching relationship network: {e}")
            return None

# Singleton instance
neo4j_service = Neo4jService()