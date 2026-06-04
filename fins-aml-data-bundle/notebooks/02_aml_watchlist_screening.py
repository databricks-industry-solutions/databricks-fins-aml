# Databricks notebook source
# MAGIC %md
# MAGIC # AML Data Pipeline - Watchlist Screening Bridge Table
# MAGIC
# MAGIC This notebook adds the `watchlist_screening` table that connects customers to watchlist hits.
# MAGIC Run this AFTER the main data generation notebook.
# MAGIC
# MAGIC ## Table Relationships (Complete)
# MAGIC
# MAGIC ```
# MAGIC customers (1) ──────► (N) accounts (1) ──────► (N) transactions
# MAGIC     │                       │
# MAGIC     │                       │
# MAGIC     ▼                       ▼
# MAGIC     └──────────────► alerts (N)
# MAGIC                         │
# MAGIC                         ▼
# MAGIC                      cases (1) ──────► (1) sar_filings
# MAGIC
# MAGIC customers (1) ──────► (N) watchlist_screening (N) ◄────── (1) watchlists
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Required Packages

# COMMAND ----------

# Install required packages
# Note: In serverless environments, packages must be installed without Python restart
%pip install faker --quiet

# Check if we're in a serverless environment by trying to restart Python
try:
    dbutils.library.restartPython()
except Exception as e:
    # In serverless, restartPython is not supported - continue without restart
    print("Note: Running in serverless environment - continuing without Python restart")
    pass

# COMMAND ----------

# Create widgets for parameters
dbutils.widgets.text("catalog", "fins_aml", "Catalog Name")
dbutils.widgets.text("schema", "data_generation", "Schema Name")

# Get parameters from widgets
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

print(f"Parameters: CATALOG={CATALOG}, SCHEMA={SCHEMA}")

# COMMAND ----------

# Set up catalog and schema
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"✅ Using catalog: {CATALOG}, schema: {SCHEMA}")

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timedelta
from faker import Faker
import random

fake = Faker()
Faker.seed(42)
random.seed(42)

# Load existing tables
customers_df = spark.table(f"{CATALOG}.{SCHEMA}.customers")
watchlists_df = spark.table(f"{CATALOG}.{SCHEMA}.watchlists")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Watchlist Screening Results
# MAGIC
# MAGIC This table represents the results of screening customers against watchlists (OFAC, PEP, etc.)

# COMMAND ----------

def generate_watchlist_screenings(customers_df, watchlists_df):
    """
    Generate watchlist screening results linking customers to potential watchlist matches.
    
    In reality, this would come from a screening system like:
    - LexisNexis WorldCompliance
    - Dow Jones Risk & Compliance
    - Refinitiv World-Check
    """
    
    screenings = []
    screening_id = 1
    
    customers = customers_df.collect()
    watchlist_entries = watchlists_df.collect()
    
    # Group watchlist entries by type for realistic matching
    ofac_entries = [w for w in watchlist_entries if w.list_type == "OFAC_SDN"]
    pep_entries = [w for w in watchlist_entries if w.list_type == "PEP"]
    internal_entries = [w for w in watchlist_entries if w.list_type == "INTERNAL"]
    
    for customer in customers:
        customer_id = customer.customer_id
        risk_rating = customer.risk_rating
        pep_flag = customer.pep_flag
        
        # Determine screening frequency based on risk
        # High risk = screened more often, more likely to have hits
        if risk_rating == "high":
            num_screenings = random.randint(2, 4)  # Multiple screening events
            hit_probability = 0.4  # 40% chance of at least one hit
        elif risk_rating == "medium":
            num_screenings = random.randint(1, 2)
            hit_probability = 0.15
        else:
            num_screenings = 1
            hit_probability = 0.02
        
        # PEP customers always have at least one PEP screening hit
        if pep_flag:
            hit_probability = 1.0
        
        for _ in range(num_screenings):
            screening_date = fake.date_time_between(start_date="-1y", end_date="now")
            
            # Determine if this screening has a hit
            has_hit = random.random() < hit_probability
            
            if has_hit:
                # Select which watchlist type to match against
                if pep_flag and random.random() < 0.8:
                    # PEP customers mostly match PEP lists
                    matched_entry = random.choice(pep_entries) if pep_entries else random.choice(watchlist_entries)
                    match_type = "PEP"
                elif risk_rating == "high" and random.random() < 0.5:
                    # High risk might match OFAC
                    matched_entry = random.choice(ofac_entries) if ofac_entries else random.choice(watchlist_entries)
                    match_type = "OFAC_SDN"
                else:
                    # Others might match internal list
                    matched_entry = random.choice(internal_entries) if internal_entries else random.choice(watchlist_entries)
                    match_type = "INTERNAL"
                
                # Generate match score (higher = better match)
                match_score = random.randint(60, 98)
                
                # Disposition based on score and type
                if match_score >= 90:
                    match_status = "confirmed_match"
                    disposition = random.choice(["escalated", "sar_filed", "account_closed"])
                elif match_score >= 75:
                    match_status = "potential_match"
                    disposition = random.choice(["under_review", "escalated", "cleared_false_positive"])
                else:
                    match_status = "potential_match"
                    disposition = random.choice(["cleared_false_positive", "under_review"])
                
                screening = {
                    "screening_id": screening_id,
                    "customer_id": customer_id,
                    "watchlist_id": matched_entry.list_id,
                    "list_type": matched_entry.list_type,
                    "matched_name": matched_entry.entity_name,
                    "match_score": match_score,
                    "match_status": match_status,
                    "match_type": random.choice(["name", "name_dob", "name_address", "alias"]),
                    "screening_date": screening_date.isoformat(),
                    "screening_system": random.choice(["WorldCompliance", "World-Check", "Dow Jones", "Internal"]),
                    "disposition": disposition,
                    "disposition_date": (screening_date + timedelta(days=random.randint(1, 14))).isoformat() if disposition != "under_review" else None,
                    "disposition_by": random.choice(["Sarah Chen", "Michael Rodriguez", "Emily Thompson", "James Wong"]) if disposition != "under_review" else None,
                    "notes": None,
                }
                
                # Add notes for certain dispositions
                if disposition == "cleared_false_positive":
                    screening["notes"] = random.choice([
                        "Name similarity only - different DOB confirmed",
                        "Common name match - verified different individual via ID",
                        "Geographic mismatch - customer has no connection to matched country",
                        "Alias match cleared - customer provided documentation"
                    ])
                elif disposition == "escalated":
                    screening["notes"] = random.choice([
                        "Potential match requires BSA Officer review",
                        "High score match - additional documentation requested",
                        "Match to sanctions list - immediate escalation required"
                    ])
                
                screenings.append(screening)
                screening_id += 1
            
            else:
                # No hit - record clear screening
                screening = {
                    "screening_id": screening_id,
                    "customer_id": customer_id,
                    "watchlist_id": None,
                    "list_type": None,
                    "matched_name": None,
                    "match_score": 0,
                    "match_status": "no_match",
                    "match_type": None,
                    "screening_date": screening_date.isoformat(),
                    "screening_system": random.choice(["WorldCompliance", "World-Check", "Dow Jones", "Internal"]),
                    "disposition": "cleared",
                    "disposition_date": screening_date.isoformat(),
                    "disposition_by": "System",
                    "notes": "Automated clearance - no matches found",
                }
                screenings.append(screening)
                screening_id += 1
    
    return screenings

# Generate and save screenings
screenings = generate_watchlist_screenings(customers_df, watchlists_df)
screenings_df = spark.createDataFrame(screenings)
screenings_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.watchlist_screening")

print(f"Generated {len(screenings)} watchlist screening records")
display(screenings_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Relationships

# COMMAND ----------

# Show screenings with matches joined to customer and watchlist details
verification_query = f"""
SELECT 
    ws.screening_id,
    ws.customer_id,
    COALESCE(c.first_name || ' ' || c.last_name, c.business_name) as customer_name,
    c.risk_rating as customer_risk,
    ws.list_type,
    ws.matched_name,
    ws.match_score,
    ws.match_status,
    ws.disposition,
    w.country as watchlist_country,
    w.program as watchlist_program
FROM {CATALOG}.{SCHEMA}.watchlist_screening ws
JOIN {CATALOG}.{SCHEMA}.customers c ON ws.customer_id = c.customer_id
LEFT JOIN {CATALOG}.{SCHEMA}.watchlists w ON ws.watchlist_id = w.list_id
WHERE ws.match_status != 'no_match'
ORDER BY ws.match_score DESC
LIMIT 20
"""

display(spark.sql(verification_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

print("=" * 60)
print("Watchlist Screening Summary")
print("=" * 60)

summary = spark.sql(f"""
SELECT 
    match_status,
    disposition,
    COUNT(*) as count,
    ROUND(AVG(match_score), 1) as avg_score
FROM {CATALOG}.{SCHEMA}.watchlist_screening
GROUP BY match_status, disposition
ORDER BY match_status, count DESC
""")

display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete ERD
# MAGIC
# MAGIC With this table added, here's the complete entity relationship diagram:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                        AML SOLUTION DATA MODEL                              │
# MAGIC └─────────────────────────────────────────────────────────────────────────────┘
# MAGIC
# MAGIC                              ┌─────────────────┐
# MAGIC                              │   watchlists    │
# MAGIC                              │ list_id (PK)    │
# MAGIC                              │ list_type       │
# MAGIC                              │ entity_name     │
# MAGIC                              │ country         │
# MAGIC                              └────────┬────────┘
# MAGIC                                       │
# MAGIC                                       │ 1:N
# MAGIC                                       ▼
# MAGIC ┌─────────────────┐         ┌─────────────────────┐
# MAGIC │   customers     │◄───────▶│ watchlist_screening │
# MAGIC │ customer_id(PK) │   1:N   │ screening_id (PK)   │
# MAGIC │ customer_type   │         │ customer_id (FK)    │
# MAGIC │ risk_rating     │         │ watchlist_id (FK)   │
# MAGIC │ pep_flag        │         │ match_score         │
# MAGIC │ kyc_status      │         │ disposition         │
# MAGIC └───────┬─────────┘         └─────────────────────┘
# MAGIC         │
# MAGIC         │ 1:N
# MAGIC         ▼
# MAGIC ┌─────────────────┐
# MAGIC │    accounts     │
# MAGIC │ account_id (PK) │
# MAGIC │ customer_id(FK) │
# MAGIC │ account_type    │
# MAGIC │ status          │
# MAGIC └───────┬─────────┘
# MAGIC         │
# MAGIC         │ 1:N
# MAGIC         ▼
# MAGIC ┌─────────────────┐
# MAGIC │  transactions   │
# MAGIC │transaction_id(PK)│
# MAGIC │ account_id (FK) │
# MAGIC │ customer_id(FK) │
# MAGIC │ amount          │
# MAGIC │ transaction_type│
# MAGIC └─────────────────┘
# MAGIC
# MAGIC
# MAGIC ┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
# MAGIC │     alerts      │────────▶│     cases       │────────▶│  sar_filings    │
# MAGIC │ alert_id (PK)   │   1:1   │ case_id (PK)    │   1:1   │ sar_id (PK)     │
# MAGIC │ customer_id(FK) │         │ alert_id (FK)   │         │ case_id (FK)    │
# MAGIC │ account_id (FK) │         │ customer_id(FK) │         │ customer_id(FK) │
# MAGIC │ scenario_code   │         │ disposition     │         │ fincen_dcn      │
# MAGIC │ alert_status    │         │ sar_required    │         │ narrative       │
# MAGIC └─────────────────┘         └─────────────────┘         └─────────────────┘
# MAGIC         ▲                           ▲                           ▲
# MAGIC         │                           │                           │
# MAGIC         └───────────────────────────┴───────────────────────────┘
# MAGIC                                     │
# MAGIC                              customer_id (FK)
# MAGIC                                     │
# MAGIC                              ┌──────┴──────┐
# MAGIC                              │  customers  │
# MAGIC                              └─────────────┘
# MAGIC ```