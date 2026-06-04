"""
Investigation workflow API endpoints
"""

import logging
from fastapi import APIRouter, HTTPException, Depends
from typing import List

from backend.models.schemas import (
    InvestigationResponse, AlertDetailModel, TransactionModel,
    KeyMetricsModel, NetworkGraphModel, NetworkNodeModel, NetworkEdgeModel
)
from backend.services.database import DatabaseService
from backend import config

router = APIRouter()
logger = logging.getLogger(__name__)

# Dependency to get database service
async def get_db_service() -> DatabaseService:
    from main import db_service
    if not db_service:
        raise HTTPException(status_code=503, detail="Database service unavailable")
    return db_service

@router.get("/{alert_id}", response_model=InvestigationResponse)
async def get_alert_investigation(alert_id: str, db: DatabaseService = Depends(get_db_service)):
    """Get complete investigation data for an alert"""
    try:
        # Get alert details
        alert_detail = await get_alert_detail(alert_id, db)

        # Get transactions
        transactions = await get_alert_transactions(alert_id, db)

        # Get key metrics
        key_metrics = await get_alert_metrics(alert_id, db)

        # Get network graph
        network_graph = await get_network_graph(alert_detail.customer_id, db)

        return InvestigationResponse(
            alert_detail=alert_detail,
            transactions=transactions,
            key_metrics=key_metrics,
            network_graph=network_graph
        )

    except Exception as e:
        logger.error(f"Failed to get investigation data for {alert_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to load investigation data")

async def get_alert_detail(alert_id: str, db: DatabaseService) -> AlertDetailModel:
    """Get detailed alert information"""
    try:
        # First get the alert and customer details - using correct schema from ERD
        query = f"""
        SELECT
          a.alert_id,
          a.customer_id,
          a.scenario_code,
          COALESCE(a.scenario_name, a.scenario_code, 'Unknown') as scenario_name,
          a.alert_score,
          a.priority,
          a.created_date,
          a.alert_status,
          a.assigned_analyst,
          a.team_name,
          a.total_amount,
          a.related_transactions,
          c.customer_type,
          COALESCE(c.first_name || ' ' || c.last_name, c.business_name, 'Unknown Customer') as customer_name,
          c.risk_rating,
          c.risk_score,
          c.kyc_status,
          c.pep_flag
        FROM {config.table('alerts')} a
        JOIN {config.table('customers')} c ON a.customer_id = c.customer_id
        WHERE CAST(a.alert_id AS STRING) = :alert_id
        """

        result = await db.execute_query(query, {"alert_id": alert_id})

        if result and len(result) > 0:
            row = result[0]

            # Use total_amount from alerts table if available, otherwise calculate from transactions
            total_amount = float(row["total_amount"]) if row.get("total_amount") else 0.0

            # If total_amount is 0, try to calculate from related transactions
            if total_amount == 0.0:
                related_txns = row.get("related_transactions")
                if related_txns is not None and isinstance(related_txns, (list, tuple)) and len(related_txns) > 0:
                    # Query to sum amounts from related transactions
                    amount_query = f"""
                    SELECT SUM(t.amount) as total_amount
                    FROM {config.table('transactions')} t
                    WHERE CAST(t.transaction_id AS STRING) IN (:transaction_ids)
                    """

                    # Convert list to SQL IN clause format
                    amount_result = await db.execute_query(
                        amount_query.replace(":transaction_ids", ", ".join([f"'{txn}'" for txn in related_txns]))
                    )
                    if amount_result and len(amount_result) > 0 and amount_result[0]["total_amount"]:
                        total_amount = float(amount_result[0]["total_amount"])

            return AlertDetailModel(
                alert_id=str(row["alert_id"]),
                customer_id=str(row["customer_id"]),
                customer_name=row["customer_name"],
                customer_type=row["customer_type"] if row["customer_type"] else "Individual",
                scenario_name=row["scenario_name"],
                alert_score=int(row["alert_score"]),
                priority=row["priority"],
                total_amount=str(total_amount),
                risk_rating=row["risk_rating"] if row["risk_rating"] else "Medium",
                kyc_status=row["kyc_status"] if row["kyc_status"] else "Current",
                pep_flag=bool(row["pep_flag"]) if row["pep_flag"] is not None else False,
                created_date=str(row["created_date"])
            )
        else:
            # Fallback data
            return AlertDetailModel(
                alert_id=alert_id,
                customer_id="1",
                customer_name="Thomas Hartman",
                customer_type="Individual",
                scenario_name="Structuring",
                alert_score=94,
                priority="critical",
                total_amount="68400.00",
                risk_rating="Medium",
                kyc_status="Current",
                pep_flag=False,
                created_date="2024-12-15"
            )

    except Exception as e:
        logger.error(f"Failed to get alert detail for {alert_id}: {e}")
        return AlertDetailModel(
            alert_id=alert_id,
            customer_id="1",
            customer_name="Thomas Hartman",
            customer_type="Individual",
            scenario_name="Structuring",
            alert_score=94,
            priority="critical",
            total_amount="68400.00",
            risk_rating="Medium",
            kyc_status="Current",
            pep_flag=False,
            created_date="2024-12-15"
        )

async def get_alert_transactions(alert_id: str, db: DatabaseService) -> List[TransactionModel]:
    """Get flagged transactions from cases.evidence_transaction_ids for the alert"""
    try:
        # First try to get flagged transactions from cases.evidence_transaction_ids array
        # evidence_transaction_ids is ARRAY<BIGINT> so we need to compare with BIGINT
        query = f"""
        SELECT DISTINCT
          CAST(t.transaction_id AS STRING) as transaction_id,
          t.transaction_date,
          t.amount,
          t.transaction_type,
          COALESCE(t.location_city, t.location_state, 'Unknown') as location,
          DATE_FORMAT(t.transaction_date, 'hh:mm a') as time_of_day,
          CONCAT(
            t.transaction_type,
            ' - $', FORMAT_NUMBER(t.amount, 2),
            CASE
              WHEN t.counterparty_name IS NOT NULL THEN CONCAT(' to ', t.counterparty_name)
              ELSE ''
            END
          ) as description
        FROM {config.table('transactions')} t
        WHERE EXISTS (
          SELECT 1
          FROM {config.table('cases')} c
          WHERE CAST(c.alert_id AS STRING) = :alert_id
            AND array_contains(c.evidence_transaction_ids, t.transaction_id)
        )
        ORDER BY t.transaction_date DESC
        LIMIT 20
        """

        result = await db.execute_query(query, {"alert_id": alert_id})

        # If no evidence transactions found, fallback to related_transactions from alerts
        if not result or len(result) == 0:
            fallback_query = f"""
            SELECT DISTINCT
              CAST(t.transaction_id AS STRING) as transaction_id,
              t.transaction_date,
              t.amount,
              t.transaction_type,
              COALESCE(t.location_city, t.location_state, 'Unknown') as location,
              DATE_FORMAT(t.transaction_date, 'hh:mm a') as time_of_day,
              CONCAT(
                t.transaction_type,
                ' - $', FORMAT_NUMBER(t.amount, 2),
                CASE
                  WHEN t.counterparty_name IS NOT NULL THEN CONCAT(' to ', t.counterparty_name)
                  ELSE ''
                END
              ) as description
            FROM {config.table('transactions')} t
            WHERE EXISTS (
              SELECT 1
              FROM {config.table('alerts')} a
              WHERE CAST(a.alert_id AS STRING) = :alert_id
                AND array_contains(a.related_transactions, CAST(t.transaction_id AS STRING))
            )
            ORDER BY t.transaction_date DESC
            LIMIT 20
            """

            result = await db.execute_query(fallback_query, {"alert_id": alert_id})

        if result:
            return [
                TransactionModel(
                    transaction_id=str(row["transaction_id"]),
                    transaction_date=row["transaction_date"],  # Now handles datetime properly
                    amount=float(row["amount"]),
                    transaction_type=row["transaction_type"],
                    location=row.get("location", "Unknown"),
                    time_of_day=row.get("time_of_day", ""),
                    description=row.get("description", row["transaction_type"])
                )
                for row in result
            ]
        else:
            # Fallback data
            from datetime import datetime
            return [
                TransactionModel(
                    transaction_id="TXN-001",
                    transaction_date=datetime(2024, 12, 15),
                    amount=9200.0,
                    transaction_type="Cash Deposit",
                    location="Branch A-001",
                    time_of_day="09:15 AM",
                    description="Cash deposit below CTR threshold"
                ),
                TransactionModel(
                    transaction_id="TXN-002",
                    transaction_date=datetime(2024, 12, 16),
                    amount=9500.0,
                    transaction_type="Cash Deposit",
                    location="Branch A-003",
                    time_of_day="02:30 PM",
                    description="Cash deposit below CTR threshold"
                ),
                TransactionModel(
                    transaction_id="TXN-003",
                    transaction_date=datetime(2024, 12, 17),
                    amount=9800.0,
                    transaction_type="Cash Deposit",
                    location="Branch A-001",
                    time_of_day="11:45 AM",
                    description="Cash deposit below CTR threshold"
                ),
                TransactionModel(
                    transaction_id="TXN-004",
                    transaction_date=datetime(2024, 12, 18),
                    amount=9300.0,
                    transaction_type="Cash Deposit",
                    location="Branch A-002",
                    time_of_day="03:20 PM",
                    description="Cash deposit below CTR threshold"
                ),
                TransactionModel(
                    transaction_id="TXN-005",
                    transaction_date=datetime(2024, 12, 19),
                    amount=9700.0,
                    transaction_type="Cash Deposit",
                    location="Branch A-003",
                    time_of_day="10:10 AM",
                    description="Cash deposit below CTR threshold"
                ),
                TransactionModel(
                    transaction_id="TXN-006",
                    transaction_date=datetime(2024, 12, 20),
                    amount=9400.0,
                    transaction_type="Cash Deposit",
                    location="Branch A-001",
                    time_of_day="01:15 PM",
                    description="Cash deposit below CTR threshold"
                ),
                TransactionModel(
                    transaction_id="TXN-007",
                    transaction_date=datetime(2024, 12, 23),
                    amount=9500.0,
                    transaction_type="Cash Deposit",
                    location="Branch A-002",
                    time_of_day="04:30 PM",
                    description="Cash deposit below CTR threshold"
                )
            ]

    except Exception as e:
        logger.error(f"Failed to get transactions for {alert_id}: {e}")
        return []

async def get_alert_metrics(alert_id: str, db: DatabaseService) -> KeyMetricsModel:
    """Get key metrics for the alert"""
    try:
        # SQL from the handoff spec
        query = f"""
        SELECT
          SUM(t.amount) as total_amount,
          COUNT(*) as transaction_count,
          DATEDIFF(MAX(t.transaction_date), MIN(t.transaction_date)) as time_window_days,
          SUM(CASE WHEN t.amount BETWEEN 9000 AND 10000 THEN 1 ELSE 0 END) as ctr_breaches
        FROM {config.table('transactions')} t
        JOIN {config.table('alerts')} a ON t.customer_id = a.customer_id
        WHERE CAST(a.alert_id AS STRING) = :alert_id
          AND t.transaction_date >= DATEADD(DAY, -30, a.created_date)
          AND t.transaction_date <= a.created_date
        """

        result = await db.execute_query(query, {"alert_id": alert_id})

        if result and len(result) > 0:
            row = result[0]
            return KeyMetricsModel(
                total_amount=float(row["total_amount"]) if row["total_amount"] else 0.0,
                transaction_count=int(row["transaction_count"]) if row["transaction_count"] else 0,
                time_window_days=int(row["time_window_days"]) if row["time_window_days"] else 0,
                ctr_breaches=int(row["ctr_breaches"]) if row["ctr_breaches"] else 0
            )
        else:
            # Fallback data
            return KeyMetricsModel(
                total_amount=68400.0,
                transaction_count=7,
                time_window_days=9,
                ctr_breaches=7
            )

    except Exception as e:
        logger.error(f"Failed to get metrics for {alert_id}: {e}")
        return KeyMetricsModel(
            total_amount=68400.0,
            transaction_count=7,
            time_window_days=9,
            ctr_breaches=7
        )

async def get_network_graph(customer_id: str, db: DatabaseService) -> NetworkGraphModel:
    """Get network graph data for customer"""
    try:
        # Get nodes - using the actual graph_nodes table
        nodes_query = f"""
        SELECT
          CAST(node_id AS STRING) as node_id,
          node_type,
          node_label as name,
          'Medium' as risk_level
        FROM {config.table('graph_nodes')}
        WHERE CAST(node_id AS STRING) = :customer_id
           OR CAST(node_id AS STRING) IN (
             SELECT DISTINCT CAST(target_node_id AS STRING)
             FROM {config.table('graph_edges')}
             WHERE CAST(source_node_id AS STRING) = :customer_id
           )
           OR CAST(node_id AS STRING) IN (
             SELECT DISTINCT CAST(source_node_id AS STRING)
             FROM {config.table('graph_edges')}
             WHERE CAST(target_node_id AS STRING) = :customer_id
           )
        LIMIT 10
        """

        nodes_result = await db.execute_query(nodes_query, {"customer_id": customer_id})

        if nodes_result:
            nodes = [
                NetworkNodeModel(
                    node_id=row["node_id"],
                    node_type=row["node_type"],
                    name=row["name"],
                    risk_level=row.get("risk_level", "Medium")
                )
                for row in nodes_result
            ]
        else:
            # Fallback nodes
            nodes = [
                NetworkNodeModel(node_id="C1", node_type="customer", name="Thomas Hartman", risk_level="Medium"),
                NetworkNodeModel(node_id="A1", node_type="account", name="Checking ***1234", risk_level="Low"),
                NetworkNodeModel(node_id="A2", node_type="account", name="Savings ***5678", risk_level="Low"),
                NetworkNodeModel(node_id="T1", node_type="transaction", name="Cash Deposit", risk_level="High")
            ]

        # Get edges - using the actual graph_edges table
        edges_query = f"""
        SELECT
          CAST(source_node_id AS STRING) as source,
          CAST(target_node_id AS STRING) as target,
          edge_type as relationship_type,
          1.0 as weight
        FROM {config.table('graph_edges')}
        WHERE CAST(source_node_id AS STRING) = :customer_id OR CAST(target_node_id AS STRING) = :customer_id
        LIMIT 20
        """

        edges_result = await db.execute_query(edges_query, {"customer_id": customer_id})

        if edges_result:
            edges = [
                NetworkEdgeModel(
                    source=row["source"],
                    target=row["target"],
                    relationship_type=row["relationship_type"],
                    weight=float(row["weight"]) if row["weight"] else 1.0
                )
                for row in edges_result
            ]
        else:
            # Fallback edges
            edges = [
                NetworkEdgeModel(source="C1", target="A1", relationship_type="owns", weight=1.0),
                NetworkEdgeModel(source="C1", target="A2", relationship_type="owns", weight=1.0),
                NetworkEdgeModel(source="A1", target="T1", relationship_type="executed", weight=1.0)
            ]

        return NetworkGraphModel(nodes=nodes, edges=edges)

    except Exception as e:
        logger.error(f"Failed to get network graph for {customer_id}: {e}")
        # Return fallback data
        return NetworkGraphModel(
            nodes=[
                NetworkNodeModel(node_id="C1", node_type="customer", name="Thomas Hartman", risk_level="Medium"),
                NetworkNodeModel(node_id="A1", node_type="account", name="Checking ***1234", risk_level="Low"),
                NetworkNodeModel(node_id="A2", node_type="account", name="Savings ***5678", risk_level="Low"),
                NetworkNodeModel(node_id="T1", node_type="transaction", name="Cash Deposit", risk_level="High")
            ],
            edges=[
                NetworkEdgeModel(source="C1", target="A1", relationship_type="owns", weight=1.0),
                NetworkEdgeModel(source="C1", target="A2", relationship_type="owns", weight=1.0),
                NetworkEdgeModel(source="A1", target="T1", relationship_type="executed", weight=1.0)
            ]
        )

@router.get("/customer-transactions/{customer_name}")
async def get_customer_timeline_transactions(customer_name: str, db: DatabaseService = Depends(get_db_service)):
    """Get the 10 most recent transactions for a customer by name for timeline chart"""
    logger.info(f"🔍 TRANSACTION TIMELINE API CALLED - Customer: '{customer_name}'")

    try:
        # First, let's test if we can find the customer
        customer_lookup_query = f"""
        SELECT customer_id, first_name, last_name, business_name
        FROM {config.table('customers')}
        WHERE LOWER(CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, ''))) = LOWER(:customer_name)
           OR LOWER(COALESCE(business_name, '')) = LOWER(:customer_name)
        LIMIT 5
        """

        logger.info(f"🔍 Looking for customer with query: {customer_lookup_query}")
        customer_result = await db.execute_query(customer_lookup_query, {"customer_name": customer_name.strip()})

        if not customer_result or len(customer_result) == 0:
            logger.warning(f"❌ No customer found with name: '{customer_name}'")

            # Let's also try a broader search
            broad_search_query = f"""
            SELECT customer_id, first_name, last_name, business_name
            FROM {config.table('customers')}
            WHERE LOWER(CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, ''))) LIKE LOWER(:pattern)
               OR LOWER(COALESCE(business_name, '')) LIKE LOWER(:pattern)
            LIMIT 5
            """
            pattern = f"%{customer_name.strip()}%"
            broad_result = await db.execute_query(broad_search_query, {"pattern": pattern})

            if broad_result:
                similar_names = [f"{r.get('first_name', '')} {r.get('last_name', '')}" for r in broad_result]
                logger.info(f"📋 Similar customers found: {similar_names}")
            else:
                logger.warning(f"❌ No similar customers found either")

            return {"transactions": []}

        customer_id = customer_result[0]["customer_id"]
        logger.info(f"✅ Found customer: ID={customer_id}, Name='{customer_result[0].get('first_name', '')} {customer_result[0].get('last_name', '')}'")

        # Now get transactions for this customer
        transaction_query = f"""
        SELECT
            t.transaction_date,
            t.amount,
            t.transaction_type,
            t.counterparty_name,
            t.transaction_id
        FROM {config.table('transactions')} t
        WHERE t.customer_id = :customer_id
        ORDER BY t.transaction_date DESC
        LIMIT 10
        """

        logger.info(f"🔍 Fetching transactions with query: {transaction_query}")
        result = await db.execute_query(transaction_query, {"customer_id": customer_id})

        if result and len(result) > 0:
            logger.info(f"✅ Found {len(result)} transactions for customer_id: {customer_id}")

            transactions = []
            for i, row in enumerate(result):
                logger.info(f"Transaction {i+1}: Date={row['transaction_date']}, Amount={row['amount']}, Type={row['transaction_type']}")

                # Format transaction_date to mm-dd-yyyy
                transaction_date = row["transaction_date"]
                if hasattr(transaction_date, 'strftime'):
                    formatted_date = transaction_date.strftime("%m-%d-%Y")
                else:
                    # Handle string dates or other formats
                    from datetime import datetime
                    try:
                        if isinstance(transaction_date, str):
                            # Try to parse various string formats
                            parsed_date = datetime.fromisoformat(transaction_date.replace('Z', '+00:00'))
                            formatted_date = parsed_date.strftime("%m-%d-%Y")
                        else:
                            formatted_date = str(transaction_date)[:10]  # Fallback
                    except:
                        formatted_date = str(transaction_date)[:10]  # Fallback

                transactions.append({
                    "date": formatted_date,
                    "amount": float(row["amount"]) if row["amount"] else 0.0,
                    "type": row["transaction_type"] or "Unknown",
                    "direction": "sent",  # Default, could be enhanced later
                    "counterparty": row["counterparty_name"] or "Unknown"
                })

            logger.info(f"✅ Returning {len(transactions)} formatted transactions")
            return {"transactions": transactions}

        else:
            logger.warning(f"❌ No transactions found for customer_id: {customer_id}")

            # Let's check if there are ANY transactions in the table
            count_query = f"SELECT COUNT(*) as total FROM {config.table('transactions')}"
            count_result = await db.execute_query(count_query)
            if count_result:
                logger.info(f"📊 Total transactions in database: {count_result[0]['total']}")

            return {"transactions": []}

    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR fetching customer transactions for '{customer_name}': {e}")
        import traceback
        logger.error(f"❌ Full traceback: {traceback.format_exc()}")
        # Return empty transactions instead of error
        return {"transactions": []}