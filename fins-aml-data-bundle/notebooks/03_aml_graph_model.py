# Databricks notebook source
# MAGIC %md
# MAGIC # AML Graph Data Model (Simplified for POC)
# MAGIC
# MAGIC This notebook creates **only two tables** for network visualization:
# MAGIC - `graph_nodes` - All entities (customers, accounts, counterparties, watchlists, alerts)
# MAGIC - `graph_edges` - All relationships between entities
# MAGIC
# MAGIC The app reads these tables directly (via SQL warehouse or, optionally,
# MAGIC Lakebase Postgres) — no separate graph database required. A Neo4j
# MAGIC reference integration is available in `/legacy/neo4j-integration/`.
# MAGIC
# MAGIC ## Graph Schema
# MAGIC ```
# MAGIC graph_nodes                          graph_edges
# MAGIC ┌─────────────────────┐              ┌─────────────────────────────┐
# MAGIC │ node_id      (PK)   │◄─────────────│ source_node_id              │
# MAGIC │ node_type    (PK)   │◄─────────────│ source_node_type            │
# MAGIC │ node_label          │              │ target_node_id              │
# MAGIC │ risk_score          │◄─────────────│ target_node_type            │
# MAGIC │ risk_category       │              │ edge_type                   │
# MAGIC │ properties (JSON)   │              │ weight                      │
# MAGIC └─────────────────────┘              │ properties (JSON)           │
# MAGIC                                      └─────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# Install required packages
%pip install faker --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Create widgets for parameters
dbutils.widgets.text("catalog", "fins_aml", "Catalog Name")
dbutils.widgets.text("schema", "data_generation", "Schema Name")

# Get parameters from widgets
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

print(f"Parameters: CATALOG={CATALOG}, SCHEMA={SCHEMA}")

# Set up catalog and schema
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"✅ Using catalog: {CATALOG}, schema: {SCHEMA}")

# Create volume if it doesn't exist
try:
    # First check if volume exists
    volumes = spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}").collect()
    volume_names = [v['volume_name'] for v in volumes]

    if 'exports' not in volume_names:
        print(f"Creating volume {CATALOG}.{SCHEMA}.exports...")
        spark.sql(f"CREATE VOLUME {CATALOG}.{SCHEMA}.exports")
        print(f"✅ Created volume: {CATALOG}.{SCHEMA}.exports")
    else:
        print(f"✅ Volume already exists: {CATALOG}.{SCHEMA}.exports")
except Exception as e:
    print(f"⚠️ Error with volume: {str(e)}")
    # Try alternative approach
    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.exports")
        print(f"✅ Created/verified volume: {CATALOG}.{SCHEMA}.exports")
    except Exception as e2:
        print(f"❌ Could not create volume: {str(e2)}")
        raise ValueError(f"Unable to create or access volume {CATALOG}.{SCHEMA}.exports. Please ensure the schema exists and you have permission to create volumes.")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from faker import Faker
import random

fake = Faker()
Faker.seed(42)
random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Build `graph_nodes` Table
# MAGIC
# MAGIC Combines all entity types into a single node table.

# COMMAND ----------

# ----- CUSTOMER NODES -----
customer_nodes = spark.table(f"{CATALOG}.{SCHEMA}.customers").select(
    F.col("customer_id").alias("node_id"),
    F.lit("customer").alias("node_type"),
    F.coalesce(
        F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")),
        F.col("business_name")
    ).alias("node_label"),
    F.col("risk_score"),
    F.col("risk_rating").alias("risk_category"),
    F.to_json(F.struct(
        F.col("customer_type"),
        F.col("occupation"),
        F.col("address_city"),
        F.col("address_state"),
        F.col("address_country"),
        F.col("pep_flag"),
        F.col("kyc_status")
    )).alias("properties")
)
print(f"Customer nodes: {customer_nodes.count()}")

# ----- ACCOUNT NODES -----
account_nodes = spark.table(f"{CATALOG}.{SCHEMA}.accounts").select(
    F.col("account_id").alias("node_id"),
    F.lit("account").alias("node_type"),
    F.concat(F.col("account_type"), F.lit(" - "), F.col("account_number")).alias("node_label"),
    F.lit(0).alias("risk_score"),
    F.col("status").alias("risk_category"),
    F.to_json(F.struct(
        F.col("account_type"),
        F.col("status"),
        F.col("current_balance"),
        F.col("currency")
    )).alias("properties")
)
print(f"Account nodes: {account_nodes.count()}")

# ----- COUNTERPARTY NODES (extracted directly from transactions) -----
counterparty_nodes = spark.table(f"{CATALOG}.{SCHEMA}.transactions").filter(
    F.col("counterparty_name").isNotNull()
).select(
    F.col("counterparty_name"),
    F.col("counterparty_account"),
    F.col("counterparty_bank"),
    F.col("counterparty_country")
).distinct().withColumn(
    "node_id",
    F.abs(F.hash(F.concat_ws("|", 
        F.col("counterparty_name"), 
        F.coalesce(F.col("counterparty_account"), F.lit("")),
        F.coalesce(F.col("counterparty_bank"), F.lit(""))
    )))
).withColumn(
    "node_type", F.lit("counterparty")
).withColumn(
    "node_label", F.col("counterparty_name")
).withColumn(
    "risk_score",
    F.when(F.col("counterparty_country").isin(["IR", "KP", "MM", "SY", "CU", "VE", "RU", "BY"]), 
           F.lit(80) + (F.rand() * 20).cast("int"))
    .otherwise(F.lit(10) + (F.rand() * 40).cast("int"))
).withColumn(
    "risk_category",
    F.when(F.col("risk_score") >= 70, "high")
     .when(F.col("risk_score") >= 40, "medium")
     .otherwise("low")
).withColumn(
    "properties",
    F.to_json(F.struct(
        F.col("counterparty_bank"),
        F.col("counterparty_country"),
        F.when(
            F.col("counterparty_name").rlike("(?i)(LLC|Inc|Corp|Ltd|Company|Bank|Trading|Holdings)"),
            F.lit("business")
        ).otherwise(F.lit("individual")).alias("entity_type")
    ))
).select("node_id", "node_type", "node_label", "risk_score", "risk_category", "properties")

print(f"Counterparty nodes: {counterparty_nodes.count()}")

# ----- WATCHLIST NODES -----
watchlist_nodes = spark.table(f"{CATALOG}.{SCHEMA}.watchlists").select(
    F.col("list_id").alias("node_id"),
    F.lit("watchlist").alias("node_type"),
    F.col("entity_name").alias("node_label"),
    F.lit(100).alias("risk_score"),
    F.col("list_type").alias("risk_category"),
    F.to_json(F.struct(
        F.col("list_type"),
        F.col("entity_type"),
        F.col("country"),
        F.col("program")
    )).alias("properties")
)
print(f"Watchlist nodes: {watchlist_nodes.count()}")

# ----- ALERT NODES -----
alert_nodes = spark.table(f"{CATALOG}.{SCHEMA}.alerts").select(
    F.col("alert_id").alias("node_id"),
    F.lit("alert").alias("node_type"),
    F.concat(F.col("scenario_code"), F.lit(": "), F.col("scenario_name")).alias("node_label"),
    F.col("alert_score").alias("risk_score"),
    F.col("priority").alias("risk_category"),
    F.to_json(F.struct(
        F.col("scenario_code"),
        F.col("alert_status"),
        F.col("priority"),
        F.col("total_amount")
    )).alias("properties")
)
print(f"Alert nodes: {alert_nodes.count()}")

# ----- UNION ALL NODES -----
all_nodes = customer_nodes \
    .unionByName(account_nodes) \
    .unionByName(counterparty_nodes) \
    .unionByName(watchlist_nodes) \
    .unionByName(alert_nodes)

all_nodes.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.graph_nodes")
print(f"\n✅ Created graph_nodes table with {all_nodes.count()} total nodes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build `graph_edges` Table
# MAGIC
# MAGIC Combines all relationship types into a single edge table.

# COMMAND ----------

# ----- OWNS_ACCOUNT: Customer -> Account -----
owns_account = spark.table(f"{CATALOG}.{SCHEMA}.accounts").select(
    F.col("customer_id").alias("source_node_id"),
    F.lit("customer").alias("source_node_type"),
    F.col("account_id").alias("target_node_id"),
    F.lit("account").alias("target_node_type"),
    F.lit("OWNS_ACCOUNT").alias("edge_type"),
    F.lit(1.0).alias("weight"),
    F.to_json(F.struct(F.col("open_date"), F.col("account_type"))).alias("properties")
)
print(f"OWNS_ACCOUNT edges: {owns_account.count()}")

# ----- HAS_ALERT: Customer -> Alert -----
has_alert = spark.table(f"{CATALOG}.{SCHEMA}.alerts").select(
    F.col("customer_id").alias("source_node_id"),
    F.lit("customer").alias("source_node_type"),
    F.col("alert_id").alias("target_node_id"),
    F.lit("alert").alias("target_node_type"),
    F.lit("HAS_ALERT").alias("edge_type"),
    (F.col("alert_score") / 100.0).alias("weight"),
    F.to_json(F.struct(F.col("created_date"), F.col("scenario_code"))).alias("properties")
)
print(f"HAS_ALERT edges: {has_alert.count()}")

# ----- MATCHES_WATCHLIST: Customer -> Watchlist -----
matches_watchlist = spark.table(f"{CATALOG}.{SCHEMA}.watchlist_screening").filter(
    F.col("watchlist_id").isNotNull() & (F.col("match_status") != "no_match")
).select(
    F.col("customer_id").alias("source_node_id"),
    F.lit("customer").alias("source_node_type"),
    F.col("watchlist_id").alias("target_node_id"),
    F.lit("watchlist").alias("target_node_type"),
    F.lit("MATCHES_WATCHLIST").alias("edge_type"),
    (F.col("match_score") / 100.0).alias("weight"),
    F.to_json(F.struct(F.col("match_status"), F.col("disposition"))).alias("properties")
)
print(f"MATCHES_WATCHLIST edges: {matches_watchlist.count()}")

# ----- WIRE/ACH TRANSFER: Customer <-> Counterparty -----
transactions_df = spark.table(f"{CATALOG}.{SCHEMA}.transactions")

# Build counterparty lookup for node_id mapping
counterparty_lookup = transactions_df.filter(
    F.col("counterparty_name").isNotNull()
).select(
    F.col("counterparty_name"),
    F.col("counterparty_account"),
    F.col("counterparty_bank")
).distinct().withColumn(
    "counterparty_node_id",
    F.abs(F.hash(F.concat_ws("|", 
        F.col("counterparty_name"), 
        F.coalesce(F.col("counterparty_account"), F.lit("")),
        F.coalesce(F.col("counterparty_bank"), F.lit(""))
    )))
)

# Join transactions with counterparty lookup
txn_with_cp = transactions_df.filter(
    F.col("transaction_type").isin(["wire_in", "wire_out", "ach_in", "ach_out"])
).join(
    counterparty_lookup,
    ["counterparty_name", "counterparty_account", "counterparty_bank"],
    "inner"
)

# Outbound transfers: Customer -> Counterparty
outbound_transfers = txn_with_cp.filter(
    F.col("transaction_type").isin(["wire_out", "ach_out"])
).select(
    F.col("customer_id").alias("source_node_id"),
    F.lit("customer").alias("source_node_type"),
    F.col("counterparty_node_id").alias("target_node_id"),
    F.lit("counterparty").alias("target_node_type"),
    F.when(F.col("transaction_type") == "wire_out", "WIRE_TRANSFER")
     .otherwise("ACH_TRANSFER").alias("edge_type"),
    F.least(F.col("amount") / 100000.0, F.lit(1.0)).alias("weight"),
    F.to_json(F.struct(
        F.col("amount"), 
        F.col("transaction_date"),
        F.col("counterparty_country")
    )).alias("properties")
)

# Inbound transfers: Counterparty -> Customer
inbound_transfers = txn_with_cp.filter(
    F.col("transaction_type").isin(["wire_in", "ach_in"])
).select(
    F.col("counterparty_node_id").alias("source_node_id"),
    F.lit("counterparty").alias("source_node_type"),
    F.col("customer_id").alias("target_node_id"),
    F.lit("customer").alias("target_node_type"),
    F.when(F.col("transaction_type") == "wire_in", "WIRE_TRANSFER")
     .otherwise("ACH_TRANSFER").alias("edge_type"),
    F.least(F.col("amount") / 100000.0, F.lit(1.0)).alias("weight"),
    F.to_json(F.struct(
        F.col("amount"), 
        F.col("transaction_date"),
        F.col("counterparty_country")
    )).alias("properties")
)

transfer_edges = outbound_transfers.unionByName(inbound_transfers)
print(f"WIRE/ACH_TRANSFER edges: {transfer_edges.count()}")

# ----- CUSTOMER RELATIONSHIPS: Customer <-> Customer -----
# Detect shared attributes directly from customers table
customers_df = spark.table(f"{CATALOG}.{SCHEMA}.customers")

# Self-join for shared employer (simplest relationship to detect)
shared_employer = customers_df.alias("c1").join(
    customers_df.alias("c2"),
    (F.col("c1.employer") == F.col("c2.employer")) & 
    (F.col("c1.employer").isNotNull()) &
    (F.col("c1.customer_id") < F.col("c2.customer_id")),  # Avoid duplicates
    "inner"
).select(
    F.col("c1.customer_id").alias("source_node_id"),
    F.lit("customer").alias("source_node_type"),
    F.col("c2.customer_id").alias("target_node_id"),
    F.lit("customer").alias("target_node_type"),
    F.lit("SHARES_EMPLOYER").alias("edge_type"),
    F.lit(0.5).alias("weight"),
    F.to_json(F.struct(F.col("c1.employer").alias("employer"))).alias("properties")
)

# Shared address
shared_address = customers_df.alias("c1").join(
    customers_df.alias("c2"),
    (F.col("c1.address_line1") == F.col("c2.address_line1")) & 
    (F.col("c1.address_city") == F.col("c2.address_city")) &
    (F.col("c1.address_line1").isNotNull()) &
    (F.col("c1.customer_id") < F.col("c2.customer_id")),
    "inner"
).select(
    F.col("c1.customer_id").alias("source_node_id"),
    F.lit("customer").alias("source_node_type"),
    F.col("c2.customer_id").alias("target_node_id"),
    F.lit("customer").alias("target_node_type"),
    F.lit("SHARES_ADDRESS").alias("edge_type"),
    F.lit(0.9).alias("weight"),
    F.to_json(F.struct(
        F.col("c1.address_line1").alias("address"),
        F.col("c1.address_city").alias("city")
    )).alias("properties")
)

# Same last name + same city (potential family)
potential_family = customers_df.alias("c1").join(
    customers_df.alias("c2"),
    (F.col("c1.last_name") == F.col("c2.last_name")) & 
    (F.col("c1.address_city") == F.col("c2.address_city")) &
    (F.col("c1.last_name").isNotNull()) &
    (F.col("c1.customer_id") < F.col("c2.customer_id")),
    "inner"
).select(
    F.col("c1.customer_id").alias("source_node_id"),
    F.lit("customer").alias("source_node_type"),
    F.col("c2.customer_id").alias("target_node_id"),
    F.lit("customer").alias("target_node_type"),
    F.lit("POTENTIAL_FAMILY").alias("edge_type"),
    F.lit(0.6).alias("weight"),
    F.to_json(F.struct(
        F.col("c1.last_name").alias("last_name"),
        F.col("c1.address_city").alias("city")
    )).alias("properties")
)

customer_rel_edges = shared_employer.unionByName(shared_address).unionByName(potential_family)
print(f"CUSTOMER_RELATIONSHIP edges: {customer_rel_edges.count()}")

# ----- UNION ALL EDGES -----
all_edges = owns_account \
    .unionByName(has_alert) \
    .unionByName(matches_watchlist) \
    .unionByName(transfer_edges) \
    .unionByName(customer_rel_edges)

# Add unique edge_id
all_edges_final = all_edges.withColumn("edge_id", F.monotonically_increasing_id())

all_edges_final.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.graph_edges")
print(f"\n✅ Created graph_edges table with {all_edges_final.count()} total edges")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate Referential Integrity

# COMMAND ----------

graph_nodes = spark.table(f"{CATALOG}.{SCHEMA}.graph_nodes")
graph_edges = spark.table(f"{CATALOG}.{SCHEMA}.graph_edges")

# Check for orphan source references
orphan_sources = graph_edges.join(
    graph_nodes,
    (graph_edges.source_node_id == graph_nodes.node_id) & 
    (graph_edges.source_node_type == graph_nodes.node_type),
    "left_anti"
)

# Check for orphan target references
orphan_targets = graph_edges.join(
    graph_nodes,
    (graph_edges.target_node_id == graph_nodes.node_id) & 
    (graph_edges.target_node_type == graph_nodes.node_type),
    "left_anti"
)

print(f"Orphan source references: {orphan_sources.count()}")
print(f"Orphan target references: {orphan_targets.count()}")

if orphan_sources.count() == 0 and orphan_targets.count() == 0:
    print("✅ Referential integrity confirmed!")
else:
    print("⚠️ Some edges reference non-existent nodes:")
    if orphan_sources.count() > 0:
        display(orphan_sources.limit(5))
    if orphan_targets.count() > 0:
        display(orphan_targets.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Customer Network View

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.v_customer_network AS
SELECT 
    e.edge_id,
    e.source_node_id,
    e.source_node_type,
    sn.node_label as source_label,
    sn.risk_score as source_risk_score,
    sn.risk_category as source_risk_category,
    e.target_node_id,
    e.target_node_type,
    tn.node_label as target_label,
    tn.risk_score as target_risk_score,
    tn.risk_category as target_risk_category,
    e.edge_type,
    e.weight,
    e.properties
FROM {CATALOG}.{SCHEMA}.graph_edges e
JOIN {CATALOG}.{SCHEMA}.graph_nodes sn 
    ON e.source_node_id = sn.node_id AND e.source_node_type = sn.node_type
JOIN {CATALOG}.{SCHEMA}.graph_nodes tn 
    ON e.target_node_id = tn.node_id AND e.target_node_type = tn.node_type
""")

print("✅ Created v_customer_network view")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary Statistics

# COMMAND ----------

print("=" * 70)
print("GRAPH DATA MODEL SUMMARY")
print("=" * 70)

# Node distribution
print("\n📊 Node Distribution:")
node_dist = spark.sql(f"""
    SELECT node_type, COUNT(*) as count, ROUND(AVG(risk_score), 1) as avg_risk
    FROM {CATALOG}.{SCHEMA}.graph_nodes 
    GROUP BY node_type 
    ORDER BY count DESC
""")
display(node_dist)

# Edge distribution
print("\n🔗 Edge Distribution:")
edge_dist = spark.sql(f"""
    SELECT edge_type, COUNT(*) as count, ROUND(AVG(weight), 2) as avg_weight
    FROM {CATALOG}.{SCHEMA}.graph_edges 
    GROUP BY edge_type 
    ORDER BY count DESC
""")
display(edge_dist)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Demo: Thomas Hartman's Network (Customer ID 1)

# COMMAND ----------

print("🔍 Network for Customer ID 1 (Thomas Hartman):")

demo_query = f"""
SELECT 
    source_label,
    edge_type,
    target_label,
    target_node_type,
    target_risk_score,
    ROUND(weight, 2) as weight
FROM {CATALOG}.{SCHEMA}.v_customer_network
WHERE (source_node_type = 'customer' AND source_node_id = 1)
   OR (target_node_type = 'customer' AND target_node_id = 1)
ORDER BY weight DESC
"""
display(spark.sql(demo_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. GraphFrames Example (Optional)
# MAGIC
# MAGIC If you want to run graph algorithms in Databricks:

# COMMAND ----------

# Uncomment to run GraphFrames analysis
# from graphframes import GraphFrame
# 
# nodes = spark.sql(f"""
#     SELECT CONCAT(node_type, '_', node_id) as id, node_type, node_label, risk_score
#     FROM {CATALOG}.{SCHEMA}.graph_nodes
# """)
# 
# edges = spark.sql(f"""
#     SELECT 
#         CONCAT(source_node_type, '_', source_node_id) as src,
#         CONCAT(target_node_type, '_', target_node_id) as dst,
#         edge_type, weight
#     FROM {CATALOG}.{SCHEMA}.graph_edges
# """)
# 
# g = GraphFrame(nodes, edges)
# 
# # PageRank - find most connected/important nodes
# pr = g.pageRank(resetProbability=0.15, maxIter=10)
# display(pr.vertices.orderBy(F.desc("pagerank")).limit(20))