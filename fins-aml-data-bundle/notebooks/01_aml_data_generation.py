# Databricks notebook source
# MAGIC %md
# MAGIC # AML Solution Accelerator - Data Generation
# MAGIC
# MAGIC This notebook generates all synthetic data for the Agentic AML System demo:
# MAGIC
# MAGIC **Core Entities:**
# MAGIC - Customer master data with realistic profiles
# MAGIC - Account information across multiple product types  
# MAGIC - Transaction history with embedded detection scenario patterns
# MAGIC
# MAGIC **Case Management:**
# MAGIC - AML alerts based on detection scenarios
# MAGIC - Investigation cases
# MAGIC - SAR filings
# MAGIC - Case audit log (for audit trail)
# MAGIC
# MAGIC **Dashboard Views:**
# MAGIC - Executive KPIs view
# MAGIC - Analyst performance view
# MAGIC - Alert backlog by scenario view
# MAGIC - Analyst queue view
# MAGIC - Customer 360 view
# MAGIC
# MAGIC **Target Schema:** `fins_aml.data_generation`
# MAGIC
# MAGIC **Execution Time:** ~15-20 minutes (includes LLM memo generation)

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

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Create widgets for parameters
dbutils.widgets.text("catalog", "fins_aml", "Catalog Name")
dbutils.widgets.text("schema", "data_generation", "Schema Name")
dbutils.widgets.text("force_rebuild", "false", "Force Rebuild (true/false)")

# Get parameters from widgets
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FORCE_REBUILD = dbutils.widgets.get("force_rebuild").lower() == "true"
NUM_CUSTOMERS = 500

print(f"Parameters: CATALOG={CATALOG}, SCHEMA={SCHEMA}, FORCE_REBUILD={FORCE_REBUILD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Catalog and Schema

# COMMAND ----------

# Check if catalog exists
catalog_exists = False
try:
    # Try to use the catalog - this will fail if it doesn't exist
    spark.sql(f"USE CATALOG {CATALOG}")
    catalog_exists = True
    print(f"✅ Using catalog: {CATALOG}")
except Exception as e:
    # Catalog doesn't exist - try to create it
    print(f"⚠️ Catalog '{CATALOG}' not found. Attempting to create it...")

    try:
        # Try simple catalog creation
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
        spark.sql(f"USE CATALOG {CATALOG}")
        print(f"✅ Created and using new catalog: {CATALOG}")
        catalog_exists = True
    except Exception as create_error:
        # Can't create catalog - likely due to permissions in shared environment
        print(f"❌ Cannot create catalog '{CATALOG}'")
        print(f"   Error: {str(create_error)[:200]}")
        print(f"\n   This is likely due to restricted permissions in this shared environment.")

        # List available catalogs for user
        print(f"\n📋 Available catalogs:")
        available_catalogs = spark.sql("SHOW CATALOGS").collect()
        user_catalogs = [cat['catalog'] for cat in available_catalogs
                        if cat['catalog'] not in ['system', 'samples', 'hive_metastore']]
        for cat in user_catalogs:
            print(f"   • {cat}")

        print(f"\n💡 Please redeploy with an existing catalog:")
        print(f"   databricks bundle run aml_data_generation_pipeline --profile fevm-fins-demo \\")
        print(f"     --var='catalog=fins_aml' --var='schema={SCHEMA}' --var='warehouse_id=<your-warehouse-id>'")

        raise ValueError(f"Catalog '{CATALOG}' does not exist and cannot be created. Please use an existing catalog.")

# COMMAND ----------

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"✅ Using catalog: {CATALOG}, schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Existing Data

# COMMAND ----------

# Check if tables already exist - with detailed debugging
def table_exists(catalog, schema, table):
    try:
        # First, let's see what tables are actually in this schema
        all_tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
        table_names = [t['tableName'] for t in all_tables]

        if table in table_names:
            print(f"  ✓ Found table '{table}' in {catalog}.{schema}")
            return True
        else:
            return False
    except Exception as e:
        print(f"  × Error checking for table '{table}': {str(e)[:100]}")
        return False

# Check core tables
tables_to_check = ["customers", "accounts", "transactions", "watchlists", "alerts", "cases", "sar_filings"]
print(f"Checking for existing tables in {CATALOG}.{SCHEMA}...")

# First show what tables are actually in this schema
try:
    existing_in_schema = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
    print(f"Tables currently in {CATALOG}.{SCHEMA}: {[t['tableName'] for t in existing_in_schema]}")
except Exception as e:
    print(f"Could not list tables in {CATALOG}.{SCHEMA}: {str(e)[:200]}")

existing_tables = [t for t in tables_to_check if table_exists(CATALOG, SCHEMA, t)]

if existing_tables and not FORCE_REBUILD:
    print(f"⚠️ Tables already exist in {CATALOG}.{SCHEMA}: {existing_tables}")
    print("Skipping data generation. Set force_rebuild=true to regenerate data.")
    dbutils.notebook.exit("Tables already exist - skipping generation")
elif existing_tables and FORCE_REBUILD:
    print(f"🔄 Force rebuild enabled. Dropping existing tables: {existing_tables}")
    for table in existing_tables:
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{table}")
else:
    print("✅ No existing tables found. Proceeding with data generation.")
NUM_MONTHS_HISTORY = 12

# LLM endpoint for memo generation
MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

# Detection scenario customer ranges (for pre-seeded patterns)
SCENARIO_RANGES = {
    "structuring": (1, 50),           # Structuring cash deposits
    "rapid_movement": (51, 100),       # Rapid in-out wire transfers
    "dormant_reactivation": (101, 150), # Dormant account reactivation
    "high_risk_geo": (151, 200),       # High-risk country transfers
    "round_dollar": (201, 250),        # Round dollar repetitive transactions
    "beneficiary_mismatch": (251, 300), # Beneficiary mismatch
    "third_party": (301, 350),         # Third-party cash deposits
    "related_accounts": (351, 400),    # Rapid movement between related accounts
    "pep_sanctions": (401, 450),       # PEP/Sanctioned entity transactions
    "normal": (451, 500),              # Normal customers (control group)
}

# Investigation teams with analysts and scenario assignments
INVESTIGATION_TEAMS = {
    "AML Transaction Monitoring": {
        "description": "Core AML alert investigation - structuring, rapid movement, round dollar, dormant reactivation",
        "scenarios": ["structuring", "rapid_movement", "round_dollar", "dormant_reactivation", "related_accounts"],
        "analysts": ["Sarah Chen", "Michael Rodriguez", "David Park", "Amanda Foster", "James Wilson"],
        "supervisors": ["Patricia Liu", "Robert Chen"]
    },
    "Enhanced Due Diligence (EDD)": {
        "description": "High-risk customers, PEPs, complex ownership, source of funds investigations",
        "scenarios": ["high_risk_geo", "beneficiary_mismatch"],
        "analysts": ["Emily Thompson", "Jennifer Adams", "Robert Martinez", "Nicole Taylor"],
        "supervisors": ["James Wong"]
    },
    "Sanctions & Watchlist Screening": {
        "description": "OFAC hits, sanctions screening, watchlist matches",
        "scenarios": ["pep_sanctions"],
        "analysts": ["Lisa Wang", "Christopher Lee", "Diana Patel"],
        "supervisors": ["Thomas Anderson"]
    },
    "Fraud Investigations": {
        "description": "Account takeover, unauthorized transactions, third-party fraud",
        "scenarios": ["third_party"],
        "analysts": ["Kevin Brown", "Maria Garcia", "Daniel Kim", "Ashley Moore"],
        "supervisors": ["Lisa Martinez"]
    }
}

# Build reverse mapping: scenario -> team
SCENARIO_TO_TEAM = {}
for team_name, team_info in INVESTIGATION_TEAMS.items():
    for scenario in team_info["scenarios"]:
        SCENARIO_TO_TEAM[scenario] = team_name

# All analysts and supervisors for reference
ALL_ANALYSTS = []
ALL_SUPERVISORS = []
for team_info in INVESTIGATION_TEAMS.values():
    ALL_ANALYSTS.extend(team_info["analysts"])
    ALL_SUPERVISORS.extend(team_info["supervisors"])
ALL_SUPERVISORS = list(set(ALL_SUPERVISORS))  # Remove duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta
from faker import Faker
import hashlib

fake = Faker()
Faker.seed(42)
random.seed(42)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate Customers

# COMMAND ----------

def generate_customers(num_customers):
    """Generate synthetic customer data with realistic profiles."""
    
    customers = []
    
    # High-risk countries for geo scenarios
    high_risk_countries = ["IR", "KP", "MM", "SY", "CU", "VE", "RU", "BY"]
    
    # Occupations by risk level
    low_risk_occupations = ["Teacher", "Nurse", "Engineer", "Accountant", "Manager", "Analyst", "Developer"]
    medium_risk_occupations = ["Real Estate Agent", "Car Dealer", "Restaurant Owner", "Import/Export", "Consultant"]
    high_risk_occupations = ["Cash Business Owner", "Money Service Business", "Jewelry Dealer", "Art Dealer", "Antique Dealer"]
    
    for customer_id in range(1, num_customers + 1):
        
        # Determine customer type (80% individual, 20% business)
        is_business = random.random() < 0.2
        
        # Determine risk profile based on scenario range
        scenario = None
        for scenario_name, (start, end) in SCENARIO_RANGES.items():
            if start <= customer_id <= end:
                scenario = scenario_name
                break
        
        # Base customer info
        if is_business:
            customer = {
                "customer_id": customer_id,
                "customer_type": "business",
                "first_name": None,
                "last_name": None,
                "business_name": fake.company(),
                "date_of_birth": None,
                "ssn_hash": hashlib.sha256(fake.ein().encode()).hexdigest()[:16],
                "address_line1": fake.street_address(),
                "address_city": fake.city(),
                "address_state": fake.state_abbr(),
                "address_zip": fake.zipcode(),
                "address_country": "US",
                "phone": fake.phone_number(),
                "email": fake.company_email(),
                "occupation": "Business Entity",
                "employer": None,
                "annual_income": random.randint(100000, 10000000),
                "source_of_wealth": random.choice(["Business Revenue", "Investment Income", "Trade Finance"]),
            }
        else:
            customer = {
                "customer_id": customer_id,
                "customer_type": "individual",
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "business_name": None,
                "date_of_birth": fake.date_of_birth(minimum_age=21, maximum_age=80).isoformat(),
                "ssn_hash": hashlib.sha256(fake.ssn().encode()).hexdigest()[:16],
                "address_line1": fake.street_address(),
                "address_city": fake.city(),
                "address_state": fake.state_abbr(),
                "address_zip": fake.zipcode(),
                "address_country": "US",
                "phone": fake.phone_number(),
                "email": fake.email(),
                "occupation": None,
                "employer": fake.company(),
                "annual_income": random.randint(30000, 500000),
                "source_of_wealth": random.choice(["Employment", "Business", "Inheritance", "Investments"]),
            }
        
        # Set occupation based on scenario for individuals
        if not is_business:
            if scenario in ["structuring", "third_party"]:
                customer["occupation"] = random.choice(high_risk_occupations)
            elif scenario in ["high_risk_geo", "pep_sanctions"]:
                customer["occupation"] = random.choice(medium_risk_occupations)
            else:
                customer["occupation"] = random.choice(low_risk_occupations)
        
        # Set risk rating
        if scenario in ["structuring", "rapid_movement", "pep_sanctions"]:
            customer["risk_rating"] = "high"
            customer["risk_score"] = random.randint(70, 95)
        elif scenario in ["high_risk_geo", "third_party", "dormant_reactivation"]:
            customer["risk_rating"] = "medium"
            customer["risk_score"] = random.randint(40, 69)
        else:
            customer["risk_rating"] = "low"
            customer["risk_score"] = random.randint(10, 39)
        
        # KYC status
        customer["kyc_status"] = random.choices(
            ["verified", "pending", "expired"],
            weights=[0.85, 0.10, 0.05]
        )[0]
        customer["kyc_date"] = fake.date_between(start_date="-2y", end_date="today").isoformat()
        
        # PEP flag (higher for pep_sanctions scenario)
        if scenario == "pep_sanctions":
            customer["pep_flag"] = random.random() < 0.6
        else:
            customer["pep_flag"] = random.random() < 0.02
        
        customer["pep_relationship"] = "Direct" if customer["pep_flag"] else None
        
        # Onboarding date
        customer["onboarding_date"] = fake.date_between(start_date="-5y", end_date="-1m").isoformat()
        customer["relationship_manager"] = f"RM-{random.randint(100, 199)}"
        
        # High-risk country address for geo scenarios
        if scenario == "high_risk_geo" and random.random() < 0.3:
            customer["address_country"] = random.choice(high_risk_countries)
        
        customers.append(customer)
    
    return customers

# Generate and save customers
customers = generate_customers(NUM_CUSTOMERS)
customers_df = spark.createDataFrame(customers)
customers_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.customers")

print(f"Generated {NUM_CUSTOMERS} customers")
display(customers_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate Accounts

# COMMAND ----------

def generate_accounts(customers_df):
    """Generate accounts for each customer."""
    
    accounts = []
    account_id = 1
    
    account_types = ["checking", "savings", "money_market", "business", "wire"]
    
    for row in customers_df.collect():
        customer_id = row.customer_id
        customer_type = row.customer_type
        onboarding_date = datetime.fromisoformat(row.onboarding_date)
        
        # Number of accounts per customer (1-5)
        if customer_type == "business":
            num_accounts = random.randint(2, 5)
        else:
            num_accounts = random.randint(1, 3)
        
        for i in range(num_accounts):
            # Determine account type
            if customer_type == "business":
                acct_type = random.choice(["business", "wire", "checking"])
            else:
                if i == 0:
                    acct_type = "checking"
                else:
                    acct_type = random.choice(["savings", "money_market", "checking"])
            
            # Account status (mostly active)
            # Dormant for dormant_reactivation scenario
            scenario = None
            for scenario_name, (start, end) in SCENARIO_RANGES.items():
                if start <= customer_id <= end:
                    scenario = scenario_name
                    break
            
            if scenario == "dormant_reactivation" and i == 0:
                status = "dormant"  # Will be reactivated in transaction generation
            else:
                status = random.choices(
                    ["active", "dormant", "closed"],
                    weights=[0.90, 0.07, 0.03]
                )[0]
            
            open_date = fake.date_between(start_date=onboarding_date, end_date="today")
            close_date = None if status != "closed" else fake.date_between(start_date=open_date, end_date="today")
            
            account = {
                "account_id": account_id,
                "customer_id": customer_id,
                "account_type": acct_type,
                "account_number": f"XXXX{random.randint(1000, 9999)}",
                "status": status,
                "open_date": open_date.isoformat(),
                "close_date": close_date.isoformat() if close_date else None,
                "current_balance": round(random.uniform(100, 500000), 2),
                "average_balance": round(random.uniform(100, 300000), 2),
                "currency": "USD",
                "branch_id": f"BR-{random.randint(100, 199)}",
            }
            
            accounts.append(account)
            account_id += 1
    
    return accounts

# Load customers and generate accounts
customers_df = spark.table(f"{CATALOG}.{SCHEMA}.customers")
accounts = generate_accounts(customers_df)
accounts_df = spark.createDataFrame(accounts)
accounts_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.accounts")

print(f"Generated {len(accounts)} accounts")
display(accounts_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate Transactions with Detection Scenario Patterns

# COMMAND ----------

def generate_transactions(customers_df, accounts_df, num_months=12):
    """Generate transaction history with embedded detection patterns."""
    
    transactions = []
    transaction_id = 1
    
    # Get current date and start date
    end_date = datetime.now()
    start_date = end_date - timedelta(days=num_months * 30)
    
    # High-risk countries
    high_risk_countries = ["IR", "KP", "MM", "SY", "CU", "VE", "RU", "BY"]
    
    # Transaction types
    txn_types = {
        "cash_deposit": {"min": 100, "max": 50000, "channel": "branch"},
        "cash_withdrawal": {"min": 100, "max": 20000, "channel": "branch"},
        "wire_in": {"min": 1000, "max": 500000, "channel": "online"},
        "wire_out": {"min": 1000, "max": 500000, "channel": "online"},
        "ach_in": {"min": 100, "max": 100000, "channel": "online"},
        "ach_out": {"min": 100, "max": 100000, "channel": "online"},
        "check_deposit": {"min": 100, "max": 50000, "channel": "mobile"},
        "internal_transfer": {"min": 100, "max": 100000, "channel": "online"},
    }
    
    # Join customers and accounts
    customer_accounts = accounts_df.join(
        customers_df.select("customer_id", "customer_type", "risk_rating"),
        "customer_id"
    ).collect()
    
    for acct in customer_accounts:
        customer_id = acct.customer_id
        account_id = acct.account_id
        account_status = acct.status
        
        # Determine scenario
        scenario = "normal"
        for scenario_name, (start, end) in SCENARIO_RANGES.items():
            if start <= customer_id <= end:
                scenario = scenario_name
                break
        
        # Skip closed accounts
        if account_status == "closed":
            continue
        
        # Number of transactions based on risk
        if scenario == "normal":
            num_txns = random.randint(20, 100)
        else:
            num_txns = random.randint(50, 200)  # More transactions for scenarios
        
        # Generate base transactions
        for _ in range(num_txns):
            txn_type = random.choice(list(txn_types.keys()))
            txn_config = txn_types[txn_type]
            
            txn = {
                "transaction_id": transaction_id,
                "account_id": account_id,
                "customer_id": customer_id,
                "transaction_type": txn_type,
                "amount": round(random.uniform(txn_config["min"], txn_config["max"]), 2),
                "currency": "USD",
                "transaction_date": fake.date_time_between(start_date=start_date, end_date=end_date).isoformat(),
                "posted_date": None,  # Will set below
                "channel": txn_config["channel"],
                "teller_id": f"T-{random.randint(100, 199)}" if txn_config["channel"] == "branch" else None,
                "branch_id": f"BR-{random.randint(100, 199)}" if txn_config["channel"] == "branch" else None,
                "counterparty_name": fake.company() if txn_type in ["wire_in", "wire_out", "ach_in", "ach_out"] else None,
                "counterparty_account": f"XXXX{random.randint(1000, 9999)}" if txn_type in ["wire_in", "wire_out"] else None,
                "counterparty_bank": fake.company() + " Bank" if txn_type in ["wire_in", "wire_out"] else None,
                "counterparty_country": "US",
                "memo": fake.sentence(nb_words=5),
                "location_city": fake.city(),
                "location_state": fake.state_abbr(),
                "location_country": "US",
                "ip_address": fake.ipv4() if txn_config["channel"] in ["online", "mobile"] else None,
                "device_id": f"DEV-{random.randint(10000, 99999)}" if txn_config["channel"] in ["online", "mobile"] else None,
                "third_party_flag": False,
                "third_party_name": None,
            }
            
            # Set posted date
            txn_dt = datetime.fromisoformat(txn["transaction_date"])
            txn["posted_date"] = (txn_dt + timedelta(days=random.randint(0, 2))).date().isoformat()
            
            transactions.append(txn)
            transaction_id += 1
        
        # ============================================
        # INJECT SCENARIO-SPECIFIC PATTERNS
        # ============================================
        
        # Recent date range for scenario patterns (last 30 days)
        recent_start = end_date - timedelta(days=30)
        
        if scenario == "structuring":
            # Pattern: ≥3 cash deposits between $9,000-$9,999 in 7 days
            pattern_start = recent_start + timedelta(days=random.randint(0, 20))
            branches = ["Main St Branch", "Oak Ave Branch", "Pine Rd Branch"]
            
            for i in range(random.randint(3, 6)):
                txn = {
                    "transaction_id": transaction_id,
                    "account_id": account_id,
                    "customer_id": customer_id,
                    "transaction_type": "cash_deposit",
                    "amount": round(random.uniform(9000, 9999), 2),
                    "currency": "USD",
                    "transaction_date": (pattern_start + timedelta(days=i)).isoformat(),
                    "posted_date": (pattern_start + timedelta(days=i)).date().isoformat(),
                    "channel": "branch",
                    "teller_id": f"T-{random.randint(100, 199)}",
                    "branch_id": f"BR-{random.randint(100, 103)}",
                    "counterparty_name": None,
                    "counterparty_account": None,
                    "counterparty_bank": None,
                    "counterparty_country": None,
                    "memo": "Cash deposit",
                    "location_city": fake.city(),
                    "location_state": fake.state_abbr(),
                    "location_country": "US",
                    "ip_address": None,
                    "device_id": None,
                    "third_party_flag": False,
                    "third_party_name": None,
                }
                transactions.append(txn)
                transaction_id += 1
        
        elif scenario == "rapid_movement":
            # Pattern: Inflow + Outflow > $50K in 24 hrs, ending balance < 5%
            pattern_date = recent_start + timedelta(days=random.randint(0, 25))
            inflow_amount = round(random.uniform(50000, 200000), 2)
            outflow_amount = round(inflow_amount * random.uniform(0.95, 0.99), 2)
            
            # Wire in
            transactions.append({
                "transaction_id": transaction_id,
                "account_id": account_id,
                "customer_id": customer_id,
                "transaction_type": "wire_in",
                "amount": inflow_amount,
                "currency": "USD",
                "transaction_date": pattern_date.isoformat(),
                "posted_date": pattern_date.date().isoformat(),
                "channel": "online",
                "teller_id": None,
                "branch_id": None,
                "counterparty_name": fake.company(),
                "counterparty_account": f"XXXX{random.randint(1000, 9999)}",
                "counterparty_bank": fake.company() + " Bank",
                "counterparty_country": random.choice(["US", "UK", "DE", "AE"]),
                "memo": "Wire transfer",
                "location_city": None,
                "location_state": None,
                "location_country": "US",
                "ip_address": fake.ipv4(),
                "device_id": f"DEV-{random.randint(10000, 99999)}",
                "third_party_flag": False,
                "third_party_name": None,
            })
            transaction_id += 1
            
            # Wire out within 24 hours
            transactions.append({
                "transaction_id": transaction_id,
                "account_id": account_id,
                "customer_id": customer_id,
                "transaction_type": "wire_out",
                "amount": outflow_amount,
                "currency": "USD",
                "transaction_date": (pattern_date + timedelta(hours=random.randint(1, 20))).isoformat(),
                "posted_date": (pattern_date + timedelta(days=1)).date().isoformat(),
                "channel": "online",
                "teller_id": None,
                "branch_id": None,
                "counterparty_name": fake.company(),
                "counterparty_account": f"XXXX{random.randint(1000, 9999)}",
                "counterparty_bank": fake.company() + " Bank",
                "counterparty_country": random.choice(["US", "HK", "SG", "AE"]),
                "memo": "Wire transfer",
                "location_city": None,
                "location_state": None,
                "location_country": "US",
                "ip_address": fake.ipv4(),
                "device_id": f"DEV-{random.randint(10000, 99999)}",
                "third_party_flag": False,
                "third_party_name": None,
            })
            transaction_id += 1
        
        elif scenario == "high_risk_geo":
            # Pattern: Outgoing wire > $10K to FATF blacklisted country
            for _ in range(random.randint(1, 3)):
                pattern_date = recent_start + timedelta(days=random.randint(0, 25))
                transactions.append({
                    "transaction_id": transaction_id,
                    "account_id": account_id,
                    "customer_id": customer_id,
                    "transaction_type": "wire_out",
                    "amount": round(random.uniform(10000, 100000), 2),
                    "currency": "USD",
                    "transaction_date": pattern_date.isoformat(),
                    "posted_date": pattern_date.date().isoformat(),
                    "channel": "online",
                    "teller_id": None,
                    "branch_id": None,
                    "counterparty_name": fake.company(),
                    "counterparty_account": f"XXXX{random.randint(1000, 9999)}",
                    "counterparty_bank": fake.company() + " Bank",
                    "counterparty_country": random.choice(high_risk_countries),
                    "memo": "International wire transfer",
                    "location_city": None,
                    "location_state": None,
                    "location_country": "US",
                    "ip_address": fake.ipv4(),
                    "device_id": f"DEV-{random.randint(10000, 99999)}",
                    "third_party_flag": False,
                    "third_party_name": None,
                })
                transaction_id += 1
        
        elif scenario == "round_dollar":
            # Pattern: ≥10 round-dollar transfers/day
            pattern_date = recent_start + timedelta(days=random.randint(0, 20))
            
            for i in range(random.randint(10, 15)):
                transactions.append({
                    "transaction_id": transaction_id,
                    "account_id": account_id,
                    "customer_id": customer_id,
                    "transaction_type": random.choice(["wire_out", "ach_out"]),
                    "amount": float(random.choice([1000, 2000, 5000, 10000, 15000, 20000])),
                    "currency": "USD",
                    "transaction_date": (pattern_date + timedelta(hours=i)).isoformat(),
                    "posted_date": pattern_date.date().isoformat(),
                    "channel": "online",
                    "teller_id": None,
                    "branch_id": None,
                    "counterparty_name": fake.company(),
                    "counterparty_account": f"XXXX{random.randint(1000, 9999)}",
                    "counterparty_bank": fake.company() + " Bank",
                    "counterparty_country": "US",
                    "memo": "Transfer",
                    "location_city": None,
                    "location_state": None,
                    "location_country": "US",
                    "ip_address": fake.ipv4(),
                    "device_id": f"DEV-{random.randint(10000, 99999)}",
                    "third_party_flag": False,
                    "third_party_name": None,
                })
                transaction_id += 1
        
        elif scenario == "third_party":
            # Pattern: > 3 third-party deposits in 7 days
            pattern_start = recent_start + timedelta(days=random.randint(0, 20))
            
            for i in range(random.randint(4, 7)):
                third_party_name = fake.name()
                transactions.append({
                    "transaction_id": transaction_id,
                    "account_id": account_id,
                    "customer_id": customer_id,
                    "transaction_type": "cash_deposit",
                    "amount": round(random.uniform(2000, 9500), 2),
                    "currency": "USD",
                    "transaction_date": (pattern_start + timedelta(days=random.randint(0, 6))).isoformat(),
                    "posted_date": (pattern_start + timedelta(days=random.randint(0, 6))).date().isoformat(),
                    "channel": "branch",
                    "teller_id": f"T-{random.randint(100, 199)}",
                    "branch_id": f"BR-{random.randint(100, 103)}",
                    "counterparty_name": None,
                    "counterparty_account": None,
                    "counterparty_bank": None,
                    "counterparty_country": None,
                    "memo": f"Cash deposit by {third_party_name}",
                    "location_city": fake.city(),
                    "location_state": fake.state_abbr(),
                    "location_country": "US",
                    "ip_address": None,
                    "device_id": None,
                    "third_party_flag": True,
                    "third_party_name": third_party_name,
                })
                transaction_id += 1
        
        # Additional scenarios can be added similarly...
    
    return transactions

# Generate transactions
accounts_df = spark.table(f"{CATALOG}.{SCHEMA}.accounts")
customers_df = spark.table(f"{CATALOG}.{SCHEMA}.customers")

transactions = generate_transactions(customers_df, accounts_df, NUM_MONTHS_HISTORY)
transactions_df = spark.createDataFrame(transactions)
transactions_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.transactions")

print(f"Generated {len(transactions)} transactions")
display(transactions_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Enhance Transaction Memos with LLM
# MAGIC
# MAGIC Replace Faker-generated gibberish memos with realistic LLM-generated memos based on transaction type, amount, and counterparty.

# COMMAND ----------

# Configuration for LLM memo enhancement
MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

print("Enhancing transaction memos with LLM...")
print(f"Using model: {MODEL_ENDPOINT}")
print("This may take a few minutes...")

# Create temp view for SQL operations
spark.table(f"{CATALOG}.{SCHEMA}.transactions").createOrReplaceTempView("transactions_temp")

# Get actual columns from the table (excluding memo)
columns_df = spark.sql(f"DESCRIBE {CATALOG}.{SCHEMA}.transactions")
columns = [row.col_name for row in columns_df.collect() if row.col_name != 'memo']
columns_sql = ",\n    ".join(columns)

# Generate new memos using ai_query
enhanced_df = spark.sql(f"""
SELECT 
    {columns_sql},
    ai_query(
        '{MODEL_ENDPOINT}',
        CONCAT(
            'Generate a single realistic bank transaction memo (5-15 words). ',
            'ONLY return the memo text. No quotes or explanation. ',
            'Type: ', transaction_type, 
            '. Amount: $', CAST(amount AS STRING),
            CASE WHEN counterparty_name IS NOT NULL THEN CONCAT('. Counterparty: ', counterparty_name) ELSE '' END
        )
    ) AS memo
FROM transactions_temp
""")

# Write back to table
enhanced_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.transactions")

print("✓ Transaction memos enhanced with LLM")
display(spark.table(f"{CATALOG}.{SCHEMA}.transactions").select("transaction_id", "transaction_type", "amount", "counterparty_name", "memo").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate Watchlists

# COMMAND ----------

def generate_watchlists():
    """Generate OFAC, PEP, and internal watchlist entries."""
    
    watchlist_entries = []
    list_id = 1
    
    # OFAC SDN entries
    for _ in range(200):
        watchlist_entries.append({
            "list_id": list_id,
            "list_type": "OFAC_SDN",
            "entity_type": random.choice(["individual", "entity"]),
            "entity_name": fake.name() if random.random() < 0.6 else fake.company(),
            "alias_names": [fake.name() for _ in range(random.randint(0, 3))],
            "country": random.choice(["IR", "KP", "SY", "CU", "RU", "VE", "MM", "BY"]),
            "program": random.choice(["SDGT", "IRAN", "DPRK", "SYRIA", "CUBA"]),
            "entry_date": fake.date_between(start_date="-10y", end_date="today").isoformat(),
            "source_url": "https://sanctionssearch.ofac.treas.gov/",
        })
        list_id += 1
    
    # PEP entries
    for _ in range(100):
        watchlist_entries.append({
            "list_id": list_id,
            "list_type": "PEP",
            "entity_type": "individual",
            "entity_name": fake.name(),
            "alias_names": [],
            "country": fake.country_code(),
            "program": random.choice(["Current Government Official", "Former Government Official", "Close Associate", "Family Member"]),
            "entry_date": fake.date_between(start_date="-5y", end_date="today").isoformat(),
            "source_url": "https://example-pep-database.com/",
        })
        list_id += 1
    
    # Internal watchlist
    for _ in range(50):
        watchlist_entries.append({
            "list_id": list_id,
            "list_type": "INTERNAL",
            "entity_type": random.choice(["individual", "entity"]),
            "entity_name": fake.name() if random.random() < 0.7 else fake.company(),
            "alias_names": [],
            "country": "US",
            "program": random.choice(["Prior SAR Subject", "Account Closure", "Fraud Alert", "High Risk"]),
            "entry_date": fake.date_between(start_date="-3y", end_date="today").isoformat(),
            "source_url": None,
        })
        list_id += 1
    
    return watchlist_entries

watchlists = generate_watchlists()
watchlists_df = spark.createDataFrame(watchlists)
watchlists_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.watchlists")

print(f"Generated {len(watchlists)} watchlist entries")
display(watchlists_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate Alerts

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, ArrayType, DateType, TimestampType
)

def generate_alerts(customers_df, accounts_df, transactions_df):
    """Generate AML alerts based on detection scenarios with team assignments and linked transactions."""
    
    alerts = []
    alert_id = 1
    
    # Scenario codes
    scenario_codes = {
        "structuring": ("STRUCT-001", "Cash Structuring Detection"),
        "rapid_movement": ("RAPID-001", "Rapid Fund Movement"),
        "dormant_reactivation": ("DORM-001", "Dormant Account Reactivation"),
        "high_risk_geo": ("GEO-001", "High-Risk Geography Transfer"),
        "round_dollar": ("ROUND-001", "Round Dollar Pattern"),
        "beneficiary_mismatch": ("BENE-001", "Beneficiary Mismatch"),
        "third_party": ("3PTY-001", "Third-Party Deposit Pattern"),
        "related_accounts": ("REL-001", "Related Account Movement"),
        "pep_sanctions": ("PEP-001", "PEP/Sanctions Alert"),
    }
    
    # Convert transactions to pandas for easier filtering
    txn_pdf = transactions_df.toPandas()
    
    # Get customers with their primary account
    customers = customers_df.collect()
    accounts = accounts_df.collect()
    
    customer_accounts = {}
    for acct in accounts:
        if acct.customer_id not in customer_accounts:
            customer_accounts[acct.customer_id] = acct.account_id
    
    for customer in customers:
        customer_id = customer.customer_id
        
        # Determine scenario
        scenario = "normal"
        for scenario_name, (start, end) in SCENARIO_RANGES.items():
            if start <= customer_id <= end:
                scenario = scenario_name
                break
        
        if scenario == "normal":
            continue
        
        if scenario not in scenario_codes:
            continue
        
        code, name = scenario_codes[scenario]
        
        # Get the appropriate team for this scenario
        team_name = SCENARIO_TO_TEAM.get(scenario, "AML Transaction Monitoring")
        team_info = INVESTIGATION_TEAMS[team_name]
        team_analysts = team_info["analysts"]
        
        # Get customer's transactions
        cust_txns = txn_pdf[txn_pdf["customer_id"] == customer_id]
        
        # Find related transactions based on scenario type
        related_txn_ids = []
        if scenario == "structuring":
            mask = (cust_txns["transaction_type"] == "cash_deposit") & \
                   (cust_txns["amount"] >= 9000) & (cust_txns["amount"] < 10000)
            related_txn_ids = cust_txns[mask]["transaction_id"].astype(str).tolist()
        
        elif scenario == "rapid_movement":
            mask = cust_txns["transaction_type"].isin(["wire_in", "wire_out"])
            related_txn_ids = cust_txns[mask]["transaction_id"].astype(str).tolist()
        
        elif scenario == "dormant_reactivation":
            related_txn_ids = cust_txns.tail(20)["transaction_id"].astype(str).tolist()
        
        elif scenario == "high_risk_geo":
            high_risk = ["IR", "KP", "MM", "SY", "CU", "VE", "RU", "BY"]
            mask = cust_txns["counterparty_country"].isin(high_risk)
            related_txn_ids = cust_txns[mask]["transaction_id"].astype(str).tolist()
            if not related_txn_ids:
                mask = cust_txns["transaction_type"].isin(["wire_in", "wire_out"])
                related_txn_ids = cust_txns[mask]["transaction_id"].astype(str).tolist()
        
        elif scenario == "round_dollar":
            mask = cust_txns["amount"].apply(lambda x: x % 1000 == 0)
            related_txn_ids = cust_txns[mask]["transaction_id"].astype(str).tolist()
        
        elif scenario == "beneficiary_mismatch":
            mask = cust_txns["transaction_type"].isin(["wire_out", "ach_out"])
            related_txn_ids = cust_txns[mask]["transaction_id"].astype(str).tolist()
        
        elif scenario == "third_party":
            mask = cust_txns["transaction_type"] == "cash_deposit"
            related_txn_ids = cust_txns[mask]["transaction_id"].astype(str).tolist()
        
        elif scenario == "related_accounts":
            mask = cust_txns["transaction_type"] == "internal_transfer"
            related_txn_ids = cust_txns[mask]["transaction_id"].astype(str).tolist()
            if not related_txn_ids:
                mask = cust_txns["transaction_type"].isin(["wire_in", "wire_out"])
                related_txn_ids = cust_txns[mask]["transaction_id"].astype(str).tolist()
        
        elif scenario == "pep_sanctions":
            mask = cust_txns["amount"] >= 5000
            related_txn_ids = cust_txns[mask]["transaction_id"].astype(str).tolist()
        
        # Fallback if no transactions found
        if not related_txn_ids:
            related_txn_ids = cust_txns.sample(min(10, len(cust_txns)))["transaction_id"].astype(str).tolist()
        
        related_txn_ids = related_txn_ids[:50]  # Max 50 per alert
        
        # Calculate actual total amount from related transactions
        if related_txn_ids:
            txn_ids_int = [int(tid) for tid in related_txn_ids]
            total_amount = cust_txns[cust_txns["transaction_id"].isin(txn_ids_int)]["amount"].sum()
        else:
            total_amount = round(random.uniform(10000, 500000), 2)
        
        # Generate 1-3 alerts per scenario customer
        num_alerts = random.randint(1, 3)
        
        for i in range(num_alerts):
            alert_date = fake.date_time_between(start_date="-30d", end_date="now")
            
            # Split transactions among multiple alerts
            if num_alerts > 1 and len(related_txn_ids) > 5:
                chunk_size = len(related_txn_ids) // num_alerts
                start_idx = i * chunk_size
                end_idx = start_idx + chunk_size if i < num_alerts - 1 else len(related_txn_ids)
                alert_txn_ids = related_txn_ids[start_idx:end_idx]
                txn_ids_int = [int(tid) for tid in alert_txn_ids]
                alert_amount = cust_txns[cust_txns["transaction_id"].isin(txn_ids_int)]["amount"].sum()
            else:
                alert_txn_ids = related_txn_ids
                alert_amount = total_amount
            
            alert = {
                "alert_id": alert_id,
                "customer_id": customer_id,
                "account_id": customer_accounts.get(customer_id),
                "scenario_code": code,
                "scenario_name": name,
                "alert_score": random.randint(50, 95),
                "alert_status": random.choice(["new", "assigned", "in_progress", "escalated", "closed"]),
                "priority": random.choices(["critical", "high", "medium", "low"], weights=[0.1, 0.3, 0.4, 0.2])[0],
                "team_name": team_name,
                "assigned_analyst": random.choice(team_analysts),
                "created_date": alert_date.isoformat(),
                "due_date": (alert_date + timedelta(days=random.choice([5, 10, 15, 30]))).date().isoformat(),
                "resolution": None,
                "resolution_date": None,
                "related_transactions": alert_txn_ids,
                "total_amount": round(float(alert_amount), 2),
            }
            
            if alert["alert_status"] == "closed":
                alert["resolution"] = random.choice(["SAR Filed", "False Positive", "No Further Action", "Enhanced Monitoring"])
                alert["resolution_date"] = (alert_date + timedelta(days=random.randint(1, 20))).isoformat()
            
            alerts.append(alert)
            alert_id += 1
    
    return alerts

# COMMAND ----------

alerts_schema = StructType([
    StructField("alert_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("account_id", StringType(), True),
    StructField("scenario_code", StringType(), False),
    StructField("scenario_name", StringType(), False),
    StructField("alert_score", IntegerType(), False),
    StructField("alert_status", StringType(), False),
    StructField("priority", StringType(), False),
    StructField("team_name", StringType(), False),
    StructField("assigned_analyst", StringType(), False),
    StructField("created_date", StringType(), False),
    StructField("due_date", StringType(), False),
    StructField("resolution", StringType(), True),
    StructField("resolution_date", StringType(), True),
    StructField("related_transactions", ArrayType(StringType()), True),
    StructField("total_amount", DoubleType(), False),
])

transactions_df = spark.table(f"{CATALOG}.{SCHEMA}.transactions")
customers_df = spark.table(f"{CATALOG}.{SCHEMA}.customers")
accounts_df = spark.table(f"{CATALOG}.{SCHEMA}.accounts")

alerts = generate_alerts(customers_df, accounts_df, transactions_df)
alerts_df = spark.createDataFrame(alerts, schema=alerts_schema)
alerts_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.alerts")

print(f"Generated {len(alerts)} alerts")
display(alerts_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Generate Cases and SAR Filings

# COMMAND ----------

def generate_cases_and_sars(alerts_df, customers_df):
    """Generate investigation cases and SAR filings from escalated alerts."""
    
    cases = []
    sars = []
    case_id = 1
    sar_id = 1
    
    # Filter to escalated/closed alerts
    escalated_alerts = alerts_df.filter(
        F.col("alert_status").isin(["escalated", "closed"])
    ).collect()
    
    for alert in escalated_alerts:
        case_date = datetime.fromisoformat(alert.created_date)
        
        # Get supervisor from the same team as the alert
        team_name = alert.team_name
        team_info = INVESTIGATION_TEAMS.get(team_name, INVESTIGATION_TEAMS["AML Transaction Monitoring"])
        supervisor = random.choice(team_info["supervisors"])
        
        case = {
            "case_id": case_id,
            "alert_id": alert.alert_id,
            "customer_id": alert.customer_id,
            "case_type": alert.scenario_code.split("-")[0].lower(),
            "case_status": random.choice(["open", "pending_review", "escalated", "sar_filed", "closed_no_action"]),
            "priority": alert.priority,
            "team_name": team_name,
            "assigned_analyst": alert.assigned_analyst,
            "supervisor": supervisor,
            "open_date": case_date.isoformat(),
            "close_date": None,
            "disposition": None,
            "disposition_reason": None,
            "sar_required": False,
            "account_action": "none",
        }
        
        # Set closure info for closed cases
        if case["case_status"] in ["sar_filed", "closed_no_action"]:
            case["close_date"] = (case_date + timedelta(days=random.randint(3, 30))).isoformat()
            
            if case["case_status"] == "sar_filed":
                case["disposition"] = "SAR Filed"
                case["disposition_reason"] = "Suspicious activity confirmed - pattern consistent with money laundering indicators"
                case["sar_required"] = True
                case["account_action"] = random.choice(["enhanced_monitoring", "restrict", "close"])
                
                # Generate SAR
                sar = {
                    "sar_id": sar_id,
                    "case_id": case_id,
                    "customer_id": alert.customer_id,
                    "filing_type": "initial",
                    "filing_date": (case_date + timedelta(days=random.randint(5, 25))).date().isoformat(),
                    "fincen_dcn": f"DCN-{random.randint(10000000, 99999999)}",
                    "bsa_id": f"BSA-{random.randint(1000000, 9999999)}",
                    "activity_start": (case_date - timedelta(days=random.randint(30, 90))).date().isoformat(),
                    "activity_end": case_date.date().isoformat(),
                    "activity_type": alert.scenario_name,
                    "amount_involved": alert.total_amount,
                    "instrument_types": ["Cash", "Wire Transfer"],
                    "law_enforcement_contact": random.random() < 0.1,
                    "narrative": f"This SAR is being filed to report {alert.scenario_name.lower()} activity...",
                    "prior_sar_dcn": None,
                }
                sars.append(sar)
                sar_id += 1
            
            else:
                case["disposition"] = "No Further Action"
                case["disposition_reason"] = "Activity determined to be legitimate business operations"
        
        cases.append(case)
        case_id += 1
    
    return cases, sars

alerts_df = spark.table(f"{CATALOG}.{SCHEMA}.alerts")
cases, sars = generate_cases_and_sars(alerts_df, customers_df)

cases_df = spark.createDataFrame(cases)
cases_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.cases")

sar_schema = StructType([
    StructField("sar_id", IntegerType(), False),
    StructField("case_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("filing_type", StringType(), False),
    StructField("filing_date", StringType(), False),
    StructField("fincen_dcn", StringType(), False),
    StructField("bsa_id", StringType(), False),
    StructField("activity_start", StringType(), False),
    StructField("activity_end", StringType(), False),
    StructField("activity_type", StringType(), False),
    StructField("amount_involved", DoubleType(), False),
    StructField("instrument_types", ArrayType(StringType()), False),
    StructField("law_enforcement_contact", BooleanType(), False),
    StructField("narrative", StringType(), False),
    StructField("prior_sar_dcn", StringType(), True),
])

sars_df = spark.createDataFrame(sars, schema = sar_schema)
sars_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.sar_filings")

print(f"Generated {len(cases)} cases and {len(sars)} SAR filings")
display(cases_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Summary

# COMMAND ----------

# Summary statistics
tables = ["customers", "accounts", "transactions", "watchlists", "alerts", "cases", "sar_filings"]

print("=" * 60)
print("AML Solution Accelerator - Data Generation Summary")
print("=" * 60)

for table in tables:
    count = spark.table(f"{CATALOG}.{SCHEMA}.{table}").count()
    print(f"{table.upper()}: {count:,} records")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Detection Scenarios
# MAGIC
# MAGIC Let's verify that the detection patterns were properly seeded.

# COMMAND ----------

# Check structuring patterns
print("STRUCTURING PATTERN CHECK (Customer IDs 1-50):")
print("-" * 50)

structuring_check = spark.sql(f"""
    SELECT 
        customer_id,
        COUNT(*) as num_deposits,
        SUM(amount) as total_amount,
        MIN(transaction_date) as first_txn,
        MAX(transaction_date) as last_txn
    FROM {CATALOG}.{SCHEMA}.transactions
    WHERE customer_id BETWEEN 1 AND 50
      AND transaction_type = 'cash_deposit'
      AND amount BETWEEN 9000 AND 9999
    GROUP BY customer_id
    HAVING COUNT(*) >= 3
    ORDER BY customer_id
    LIMIT 10
""")

display(structuring_check)

# COMMAND ----------

# Check high-risk geography patterns
print("HIGH-RISK GEOGRAPHY CHECK (Customer IDs 151-200):")
print("-" * 50)

geo_check = spark.sql(f"""
    SELECT 
        customer_id,
        counterparty_country,
        COUNT(*) as num_wires,
        SUM(amount) as total_amount
    FROM {CATALOG}.{SCHEMA}.transactions
    WHERE customer_id BETWEEN 151 AND 200
      AND transaction_type = 'wire_out'
      AND counterparty_country IN ('IR', 'KP', 'MM', 'SY', 'CU', 'VE', 'RU', 'BY')
    GROUP BY customer_id, counterparty_country
    ORDER BY customer_id
    LIMIT 10
""")

display(geo_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Enhancements
# MAGIC
# MAGIC Now we'll add additional fields and views needed to support the demo workflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Enhance Tables with Additional Fields

# COMMAND ----------

# Add assigned_date and days_in_queue to alerts
alerts_df = spark.table(f"{CATALOG}.{SCHEMA}.alerts")

alerts_enhanced = alerts_df.withColumn(
    "assigned_date",
    F.when(
        F.col("alert_status").isin(["assigned", "in_progress", "escalated", "closed"]),
        F.date_add(F.col("created_date"), F.lit(1))
    ).otherwise(None)
).withColumn(
    "days_in_queue",
    F.when(
        F.col("alert_status") == "closed",
        F.datediff(F.col("resolution_date"), F.col("created_date"))
    ).otherwise(
        F.datediff(F.current_date(), F.col("created_date"))
    )
)

alerts_enhanced.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.alerts")
print("✅ Enhanced alerts table with assigned_date and days_in_queue")

# COMMAND ----------

# Add investigation_time_hours and evidence_transaction_ids to cases
cases_df = spark.table(f"{CATALOG}.{SCHEMA}.cases")
transactions_df = spark.table(f"{CATALOG}.{SCHEMA}.transactions")

# Drop evidence_transaction_ids if it already exists (from previous run)
if "evidence_transaction_ids" in cases_df.columns:
    cases_df = cases_df.drop("evidence_transaction_ids")
if "investigation_time_hours" in cases_df.columns:
    cases_df = cases_df.drop("investigation_time_hours")

evidence_txns = transactions_df.groupBy("customer_id").agg(
    F.collect_list("transaction_id").alias("txn_ids_temp")
)

cases_enhanced = cases_df.join(
    evidence_txns, 
    "customer_id", 
    "left"
).withColumn(
    "investigation_time_hours",
    F.when(
        F.col("case_status").isin(["sar_filed", "closed_no_action"]),
        F.round(F.rand() * 7.5 + 0.5, 1)
    ).otherwise(None)
).withColumn(
    "evidence_transaction_ids",
    F.slice(F.col("txn_ids_temp"), 1, 50)
).drop("txn_ids_temp")

cases_enhanced.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.cases")
print("✅ Enhanced cases table with investigation_time_hours and evidence_transaction_ids")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Generate Case Audit Log

# COMMAND ----------

def generate_audit_logs():
    """Generate audit trail entries for cases."""
    
    cases = spark.table(f"{CATALOG}.{SCHEMA}.cases").collect()
    
    audit_entries = []
    log_id = 1
    
    analysts = ["Sarah Chen", "Michael Rodriguez", "Emily Thompson", "David Park"]
    supervisors = ["James Wong", "Lisa Martinez", "Robert Chen"]
    
    for case in cases:
        case_id = case.case_id
        open_date = datetime.fromisoformat(str(case.open_date)[:19])
        
        # Entry 1: Case Created
        audit_entries.append({
            "log_id": log_id,
            "case_id": case_id,
            "action": "CASE_CREATED",
            "action_date": open_date.isoformat(),
            "performed_by": "System",
            "old_value": None,
            "new_value": "open",
            "notes": f"Case created from alert {case.alert_id}"
        })
        log_id += 1
        
        # Entry 2: Analyst Assigned
        audit_entries.append({
            "log_id": log_id,
            "case_id": case_id,
            "action": "ANALYST_ASSIGNED",
            "action_date": (open_date + timedelta(hours=random.randint(1, 4))).isoformat(),
            "performed_by": random.choice(supervisors),
            "old_value": None,
            "new_value": case.assigned_analyst,
            "notes": "Auto-assigned based on workload balancing"
        })
        log_id += 1
        
        # Entry 3: Investigation Started
        audit_entries.append({
            "log_id": log_id,
            "case_id": case_id,
            "action": "STATUS_CHANGE",
            "action_date": (open_date + timedelta(hours=random.randint(4, 24))).isoformat(),
            "performed_by": case.assigned_analyst,
            "old_value": "open",
            "new_value": "in_progress",
            "notes": "Investigation initiated"
        })
        log_id += 1
        
        # If case is closed, add resolution entries
        if case.case_status in ["sar_filed", "closed_no_action"]:
            close_date = datetime.fromisoformat(str(case.close_date)[:19]) if case.close_date else open_date + timedelta(days=5)
            
            if case.case_status == "sar_filed":
                audit_entries.append({
                    "log_id": log_id,
                    "case_id": case_id,
                    "action": "SAR_SUBMITTED",
                    "action_date": close_date.isoformat(),
                    "performed_by": case.assigned_analyst,
                    "old_value": None,
                    "new_value": "Filed with FinCEN",
                    "notes": "SAR submitted via BSA E-Filing"
                })
                log_id += 1
            
            audit_entries.append({
                "log_id": log_id,
                "case_id": case_id,
                "action": "CASE_CLOSED",
                "action_date": close_date.isoformat(),
                "performed_by": case.supervisor,
                "old_value": case.case_status.replace("_", " "),
                "new_value": "closed",
                "notes": case.disposition_reason or "Case investigation complete"
            })
            log_id += 1
    
    return audit_entries

audit_logs = generate_audit_logs()
audit_df = spark.createDataFrame(audit_logs)
audit_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.case_audit_log")

print(f"✅ Created case_audit_log table with {len(audit_logs)} entries")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Create Dashboard Views

# COMMAND ----------

# View 1: Executive KPIs
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.v_executive_kpis AS
SELECT
    COUNT(*) as total_alerts,
    SUM(CASE WHEN alert_status NOT IN ('closed') THEN 1 ELSE 0 END) as open_alerts,
    SUM(CASE WHEN alert_status = 'closed' THEN 1 ELSE 0 END) as closed_alerts,
    ROUND(
        SUM(CASE WHEN resolution = 'False Positive' THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN alert_status = 'closed' THEN 1 ELSE 0 END), 0), 
        1
    ) as false_positive_rate_pct,
    ROUND(
        SUM(CASE WHEN resolution = 'SAR Filed' THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN alert_status = 'closed' THEN 1 ELSE 0 END), 0), 
        1
    ) as sar_conversion_rate_pct,
    SUM(CASE WHEN priority = 'critical' THEN 1 ELSE 0 END) as critical_alerts,
    SUM(CASE WHEN priority = 'high' THEN 1 ELSE 0 END) as high_alerts,
    SUM(CASE WHEN days_in_queue > 5 AND alert_status NOT IN ('closed') THEN 1 ELSE 0 END) as alerts_past_sla
FROM {CATALOG}.{SCHEMA}.alerts
""")
print("✅ Created v_executive_kpis view")

# COMMAND ----------

# View 2: Analyst Performance (with team)
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.v_analyst_performance AS
SELECT
    a.team_name,
    a.assigned_analyst,
    COUNT(*) as total_assigned,
    SUM(CASE WHEN a.alert_status = 'closed' THEN 1 ELSE 0 END) as closed_count,
    SUM(CASE WHEN a.alert_status NOT IN ('closed') THEN 1 ELSE 0 END) as open_count,
    ROUND(AVG(c.investigation_time_hours), 2) as avg_investigation_hours,
    SUM(CASE WHEN a.resolution = 'SAR Filed' THEN 1 ELSE 0 END) as sars_filed,
    ROUND(
        SUM(CASE WHEN a.resolution = 'SAR Filed' THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN a.alert_status = 'closed' THEN 1 ELSE 0 END), 0),
        1
    ) as sar_rate_pct
FROM {CATALOG}.{SCHEMA}.alerts a
LEFT JOIN {CATALOG}.{SCHEMA}.cases c ON a.alert_id = c.alert_id
WHERE a.assigned_analyst IS NOT NULL
GROUP BY a.team_name, a.assigned_analyst
""")
print("✅ Created v_analyst_performance view")

# COMMAND ----------

# View 2b: Team Performance
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.v_team_performance AS
SELECT
    team_name,
    COUNT(*) as total_alerts,
    SUM(CASE WHEN alert_status = 'closed' THEN 1 ELSE 0 END) as closed_alerts,
    SUM(CASE WHEN alert_status NOT IN ('closed') THEN 1 ELSE 0 END) as open_alerts,
    ROUND(
        SUM(CASE WHEN resolution = 'False Positive' THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN alert_status = 'closed' THEN 1 ELSE 0 END), 0), 
        1
    ) as false_positive_rate_pct,
    ROUND(
        SUM(CASE WHEN resolution = 'SAR Filed' THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN alert_status = 'closed' THEN 1 ELSE 0 END), 0), 
        1
    ) as sar_conversion_rate_pct,
    SUM(CASE WHEN priority IN ('critical', 'high') AND alert_status NOT IN ('closed') THEN 1 ELSE 0 END) as high_priority_open,
    ROUND(AVG(days_in_queue), 1) as avg_days_in_queue,
    COUNT(DISTINCT assigned_analyst) as analyst_count
FROM {CATALOG}.{SCHEMA}.alerts
GROUP BY team_name
ORDER BY open_alerts DESC
""")
print("✅ Created v_team_performance view")

# COMMAND ----------

# View 3: Alert Backlog by Scenario (with team)
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.v_alert_backlog_by_scenario AS
SELECT
    team_name,
    scenario_code,
    scenario_name,
    COUNT(*) as total_alerts,
    SUM(CASE WHEN alert_status NOT IN ('closed') THEN 1 ELSE 0 END) as open_alerts,
    SUM(CASE WHEN priority IN ('critical', 'high') AND alert_status NOT IN ('closed') THEN 1 ELSE 0 END) as high_priority_open,
    AVG(days_in_queue) as avg_days_in_queue
FROM {CATALOG}.{SCHEMA}.alerts
GROUP BY team_name, scenario_code, scenario_name
ORDER BY high_priority_open DESC
""")
print("✅ Created v_alert_backlog_by_scenario view")

# COMMAND ----------

# View 4: Analyst Queue (with team)
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.v_analyst_queue AS
SELECT
    a.alert_id,
    a.team_name,
    a.assigned_analyst,
    a.customer_id,
    COALESCE(c.first_name || ' ' || c.last_name, c.business_name) as customer_name,
    a.scenario_code,
    a.scenario_name,
    a.alert_score,
    a.priority,
    a.alert_status,
    a.created_date,
    a.due_date,
    a.days_in_queue,
    a.total_amount,
    c.risk_rating as customer_risk_rating,
    c.pep_flag
FROM {CATALOG}.{SCHEMA}.alerts a
JOIN {CATALOG}.{SCHEMA}.customers c ON a.customer_id = c.customer_id
WHERE a.alert_status NOT IN ('closed')
""")
print("✅ Created v_analyst_queue view")

# COMMAND ----------

# View 5: Customer 360
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.v_customer_360 AS
SELECT
    c.*,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.accounts acc WHERE acc.customer_id = c.customer_id) as total_accounts,
    (SELECT SUM(current_balance) FROM {CATALOG}.{SCHEMA}.accounts acc WHERE acc.customer_id = c.customer_id) as total_balance,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.transactions t 
     WHERE t.customer_id = c.customer_id 
     AND t.transaction_date >= date_sub(current_date(), 90)) as txn_count_90d,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.alerts a WHERE a.customer_id = c.customer_id) as total_alerts,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.sar_filings s WHERE s.customer_id = c.customer_id) as total_sars
FROM {CATALOG}.{SCHEMA}.customers c
""")
print("✅ Created v_customer_360 view")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

print("=" * 70)
print("AML Data Generation Complete!")
print("=" * 70)

tables = ["customers", "accounts", "transactions", "watchlists", "alerts", "cases", "sar_filings", "case_audit_log"]
views = ["v_executive_kpis", "v_analyst_performance", "v_team_performance", "v_alert_backlog_by_scenario", "v_analyst_queue", "v_customer_360"]

print("\nTables Created:")
for table in tables:
    try:
        count = spark.table(f"{CATALOG}.{SCHEMA}.{table}").count()
        print(f"  ✅ {table}: {count:,} records")
    except:
        print(f"  ❌ {table}: not found")

print("\nViews Created:")
for view in views:
    print(f"  ✅ {view}")

print("\n" + "=" * 70)
print("Next Steps:")
print("  1. Run 02_aml_watchlist_screening notebook")
print("  2. Run 03_aml_graph_model notebook")
print("  3. Run 04_aml_knowledge_base notebook")
print("=" * 70)