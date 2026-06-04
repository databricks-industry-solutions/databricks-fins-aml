# Databricks notebook source
# MAGIC %md
# MAGIC # AML Knowledge Base - Document Generation & Regulatory References
# MAGIC
# MAGIC **Document Variability:**
# MAGIC | Type | Outcomes | Distribution | Analyst Attribution |
# MAGIC |------|----------|--------------|---------------------|
# MAGIC | SAR Narratives | By activity_type | From data | ✅ From linked case |
# MAGIC | Case Notes | sar_filed/closed/escalated/pending | 30/40/15/15% | ✅ From case record |
# MAGIC | EDD Memos | maintain/downgrade/escalate/exit | 50/25/15/10% | ✅ EDD team analyst |
# MAGIC | Adverse Media | none/cleared/flagged | 60/25/15% | ✅ Sanctions/EDD analyst |
# MAGIC | Correspondence | positive/neutral/minor/significant | 40/25/20/15% | ✅ Branch staff (separate) |

# COMMAND ----------

# Create widgets for parameters
dbutils.widgets.text("catalog", "fins_aml", "Catalog Name")
dbutils.widgets.text("schema", "data_generation", "Schema Name")

# Get parameters from widgets
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/knowledge_base"
MODEL_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

# Set up catalog and schema
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"✅ Using catalog: {CATALOG}, schema: {SCHEMA}")

# Create volume if it doesn't exist - robust approach with error handling
try:
    # First check if volume exists
    volumes = spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}").collect()
    volume_names = [v['volume_name'] for v in volumes]

    if 'knowledge_base' not in volume_names:
        print(f"Creating volume {CATALOG}.{SCHEMA}.knowledge_base...")
        spark.sql(f"CREATE VOLUME {CATALOG}.{SCHEMA}.knowledge_base")
        print(f"✅ Created volume: {CATALOG}.{SCHEMA}.knowledge_base")
    else:
        print(f"✅ Volume already exists: {CATALOG}.{SCHEMA}.knowledge_base")
except Exception as e:
    print(f"⚠️ Error with volume: {str(e)}")
    # Try alternative approach
    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.knowledge_base")
        print(f"✅ Created/verified volume: {CATALOG}.{SCHEMA}.knowledge_base")
    except Exception as e2:
        print(f"❌ Could not create volume: {str(e2)}")
        raise ValueError(f"Unable to create or access volume {CATALOG}.{SCHEMA}.knowledge_base. Please ensure you have the necessary permissions.")

print(f"Volume path: {VOLUME_PATH}\nModel: {MODEL_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Team Configuration
# MAGIC
# MAGIC Must match the teams defined in 01_aml_data_generation for consistency.

# COMMAND ----------

# Investigation teams - mirrors 01_aml_data_generation.py
INVESTIGATION_TEAMS = {
    "AML Transaction Monitoring": {
        "description": "Core AML alert investigation",
        "analysts": ["Sarah Chen", "Michael Rodriguez", "David Park", "Amanda Foster", "James Wilson"],
        "supervisors": ["Patricia Liu", "Robert Chen"]
    },
    "Enhanced Due Diligence (EDD)": {
        "description": "High-risk customers, PEPs, source of funds",
        "analysts": ["Emily Thompson", "Jennifer Adams", "Robert Martinez", "Nicole Taylor"],
        "supervisors": ["James Wong"]
    },
    "Sanctions & Watchlist Screening": {
        "description": "OFAC hits, sanctions screening, watchlist matches",
        "analysts": ["Lisa Wang", "Christopher Lee", "Diana Patel"],
        "supervisors": ["Thomas Anderson"]
    },
    "Fraud Investigations": {
        "description": "Account takeover, unauthorized transactions, third-party fraud",
        "analysts": ["Kevin Brown", "Maria Garcia", "Daniel Kim", "Ashley Moore"],
        "supervisors": ["Lisa Martinez"]
    }
}

# Branch staff for correspondence (separate from investigators)
BRANCH_STAFF = [
    ("Jennifer Adams", "Personal Banker"),
    ("Michael Chen", "Branch Manager"),
    ("Sarah Johnson", "Customer Service Rep"),
    ("David Williams", "Relationship Manager"),
    ("Maria Garcia", "Teller Supervisor")
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Download Regulatory Documents

# COMMAND ----------

import requests, os, time, random, pandas as pd

REGULATORY_DOCS = {
    "ffiec": [
        {"name": "appendix_f_red_flags.pdf", "url": "https://bsaaml.ffiec.gov/docs/manual/10_Appendices/07.pdf", "description": "FFIEC Red Flags"},
        {"name": "ffiec_cip_manual.pdf", "url": "https://www.fdic.gov/news/financial-institution-letters/2021/fil21012b.pdf", "description": "CIP Manual"}
    ],
    "fincen": [
        {"name": "sar_narrative_guidance.pdf", "url": "https://www.irs.gov/pub/irs-tege/itg_sarc_prep.pdf", "description": "SAR Guidance"},
        {"name": "ctr_reference_guide.pdf", "url": "https://www.fincen.gov/system/files/shared/CTRPamphlet.pdf", "description": "CTR Guide"},
        {"name": "cdd_rule_faqs.pdf", "url": "https://www.fincen.gov/system/files/2018-04/FinCEN_Guidance_CDD_FAQ_FINAL_508_2.pdf", "description": "CDD FAQs"},
        {"name": "cdd_beneficial_ownership_faqs.pdf", "url": "https://www.fincen.gov/system/files/2016-09/FAQs_for_CDD_Final_Rule_(7_15_16).pdf", "description": "BO FAQs"}
    ],
    "ofac": [
        {"name": "compliance_framework.pdf", "url": "https://ofac.treasury.gov/media/16331/download?inline=", "description": "OFAC Framework"},
        {"name": "instant_payments_guidance.pdf", "url": "https://ofac.treasury.gov/system/files/126/instant_payment_systems_compliance_guidance_brochure.pdf", "description": "Instant Payments"}
    ],
    "internal": [
        {"name": "vls_finance_aml_policy.pdf", "url": "https://www.vlsfinance.com/wp-content/uploads/2022/01/Anti-Money-Laundering-Policy.pdf", "description": "VLS Policy"},
        {"name": "jab_aml_policy.pdf", "url": "https://www.jabholco.com/img/pdf/JAB_AML_Policy.pdf", "description": "JAB Policy"},
        {"name": "hrw_aml_policy.pdf", "url": "https://www.hrw.org/sites/default/files/news_attachments/hrw-anti-money-laundering-policy-december2016.pdf", "description": "HRW Policy"},
        {"name": "multichoice_aml_policy.pdf", "url": "https://investors.multichoice.com/pdf/policies-and-charters/2024/mcg-anti-money-laundering-policy.pdf", "description": "MultiChoice Policy"}
    ]
}

def download_regulatory_docs():
    base_path = f"{VOLUME_PATH}/policies_and_regulations"
    for cat in ["ffiec", "fincen", "ofac", "internal"]:
        os.makedirs(f"{base_path}/{cat}", exist_ok=True)
    
    results = []
    for category, docs in REGULATORY_DOCS.items():
        print(f"\nDownloading {category.upper()}...")
        for doc in docs:
            try:
                headers = {'User-Agent': 'Mozilla/5.0'}
                resp = requests.get(doc["url"], timeout=60, allow_redirects=True, headers=headers)
                resp.raise_for_status()
                with open(f"{base_path}/{category}/{doc['name']}", 'wb') as f:
                    f.write(resp.content)
                results.append({"category": category, "file": doc["name"], "status": "Success", "size_kb": round(len(resp.content)/1024, 1)})
                print(f"  ✓ {doc['name']}")
            except Exception as e:
                results.append({"category": category, "file": doc["name"], "status": f"Failed: {str(e)[:30]}", "size_kb": 0})
                print(f"  ✗ {doc['name']}: {str(e)[:40]}")
    return results

results = download_regulatory_docs()
display(pd.DataFrame(results))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: LLM-Generated Documents

# COMMAND ----------

def get_customer_name(customer):
    first, last = customer.get('first_name') or '', customer.get('last_name') or ''
    business = customer.get('business_name') or ''
    if first or last: return f"{first} {last}".strip()
    elif business: return business
    else: return f"Customer {customer.get('customer_id', 'Unknown')}"

def generate_with_llm(prompt: str) -> str:
    try:
        start = time.time()
        escaped = prompt.replace("'", "''")
        result = spark.sql(f"SELECT ai_query('{MODEL_ENDPOINT}', '{escaped}') AS response").collect()[0]["response"]
        print(f"    ✓ {len(result)} chars in {time.time()-start:.1f}s")
        return result
    except Exception as e:
        print(f"    ✗ Error: {str(e)[:50]}")
        return None

# COMMAND ----------

# Load structured data
customers_df = spark.table(f"{CATALOG}.{SCHEMA}.customers").toPandas()
transactions_df = spark.table(f"{CATALOG}.{SCHEMA}.transactions").toPandas()
alerts_df = spark.table(f"{CATALOG}.{SCHEMA}.alerts").toPandas()
cases_df = spark.table(f"{CATALOG}.{SCHEMA}.cases").toPandas()
sars_df = spark.table(f"{CATALOG}.{SCHEMA}.sar_filings").toPandas()
high_risk_customers = customers_df[customers_df["risk_rating"] == "high"]

print(f"Loaded: {len(customers_df)} customers, {len(transactions_df)} txns, {len(alerts_df)} alerts, {len(cases_df)} cases, {len(sars_df)} SARs, {len(high_risk_customers)} high-risk")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 SAR Narratives
# MAGIC
# MAGIC SAR narratives include the analyst who prepared the report and their supervisor, pulled from the linked case.

# COMMAND ----------

def build_sar_prompt(customer, sar, case, transactions):
    """Build SAR prompt with analyst attribution from linked case."""
    name = get_customer_name(customer)
    txn_lines = [f"  - {str(t['transaction_date'])[:10]}: {t['transaction_type'].replace('_',' ').title()}, ${t['amount']:,.2f}" for _, t in transactions.head(10).iterrows()]
    pep = 'Yes - ' + str(customer.get('pep_relationship', 'Related')) if customer.get('pep_flag') else 'No'
    
    # Get analyst info from case
    analyst = case.get('assigned_analyst', 'AML Analyst') if case else 'AML Analyst'
    supervisor = case.get('supervisor', 'BSA Officer') if case else 'BSA Officer'
    team = case.get('team_name', 'AML Transaction Monitoring') if case else 'AML Transaction Monitoring'
    
    return f"""You are {analyst}, a BSA/AML analyst on the {team} team at ABC National Bank. Prepare a SAR narrative for FinCEN submission.

FILING INFORMATION:
- Prepared by: {analyst}, {team}
- Reviewed by: {supervisor}
- Filing Date: 2024-11-20

CUSTOMER: {name} (ID: {customer.get('customer_id')})
Occupation: {customer.get('occupation', 'N/A')} | Income: ${customer.get('annual_income', 0):,.0f} | Risk: {str(customer.get('risk_rating', 'Medium')).upper()}
Location: {customer.get('address_city', 'Unknown')}, {customer.get('address_state', 'Unknown')} | Since: {customer.get('onboarding_date', 'Unknown')} | PEP: {pep}

SAR: {sar['sar_id']} | DCN: {sar['fincen_dcn']} | Type: {sar['activity_type']}
Period: {sar['activity_start']} to {sar['activity_end']} | Amount: ${sar['amount_involved']:,.2f}

TRANSACTIONS:
{chr(10).join(txn_lines) if txn_lines else '  None'}

Generate SAR narrative with:
1) FILING HEADER (preparer, reviewer, date)
2) SUBJECT INFO
3) ACTIVITY SUMMARY
4) DETAILED NARRATIVE (5Ws)
5) TRANSACTION ANALYSIS
6) RED FLAGS (cite FFIEC Appendix F)
7) ACTIONS TAKEN
8) RECOMMENDATION
9) PREPARER CERTIFICATION (your name and title)

Write in formal regulatory language as {analyst}."""

# COMMAND ----------

sar_dir = f"{VOLUME_PATH}/sar_narratives"
os.makedirs(sar_dir, exist_ok=True)
print("="*70 + "\nGENERATING SAR NARRATIVES\n" + "="*70)

# Check if SAR narratives already exist
existing_sars = [f for f in os.listdir(sar_dir) if f.startswith("SAR_") and f.endswith(".txt")] if os.path.exists(sar_dir) else []
expected_sar_count = len(sars_df)

if len(existing_sars) >= expected_sar_count:
    print(f"✅ SAR narratives already exist: {len(existing_sars)} files found (expected {expected_sar_count})")
    print("   Skipping SAR generation to save time.")
    sar_count = len(existing_sars)
    sar_failed = 0
else:
    if existing_sars:
        print(f"⚠️  Found {len(existing_sars)} existing SARs, need {expected_sar_count}. Regenerating all...")

    sar_count, sar_failed = 0, 0
    for idx, sar in sars_df.iterrows():
        cust_row = customers_df[customers_df["customer_id"] == sar["customer_id"]]
        if cust_row.empty: continue
        customer = cust_row.iloc[0].to_dict()
        cust_txns = transactions_df[transactions_df["customer_id"] == sar["customer_id"]]

        # Get linked case for analyst info
        case_row = cases_df[cases_df["case_id"] == sar["case_id"]]
        case = case_row.iloc[0].to_dict() if not case_row.empty else None

        analyst = case.get('assigned_analyst', 'Unknown') if case else 'Unknown'
        print(f"\n[{idx+1}/{len(sars_df)}] SAR for {get_customer_name(customer)} - {sar['activity_type']} (Analyst: {analyst})")

        narrative = generate_with_llm(build_sar_prompt(customer, sar.to_dict(), case, cust_txns))

        if narrative:
            with open(f"{sar_dir}/SAR_{sar['fincen_dcn']}_customer_{sar['customer_id']:04d}.txt", 'w') as f: f.write(narrative)
            sar_count += 1
        else: sar_failed += 1

    print(f"\nSAR Narratives: {sar_count} generated, {sar_failed} failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Case Notes (Varied Outcomes)
# MAGIC
# MAGIC Case notes are written by the assigned analyst with supervisor oversight.

# COMMAND ----------

def build_case_notes_prompt(case, customer, alert, transactions, outcome):
    name = get_customer_name(customer)
    txn_lines = [f"  - {str(t['transaction_date'])[:10]}: {t['transaction_type'].replace('_',' ').title()}, ${t['amount']:,.2f}" for _, t in transactions.head(8).iterrows()]
    alert_score = alert.get('alert_score', 'N/A') if alert else 'N/A'
    alert_amount = alert.get('total_amount', 0) if alert else 0
    scenario = alert.get('scenario_name', 'Unknown') if alert else case.get('case_type', 'Unknown')
    
    # Get analyst and team info
    analyst = case.get('assigned_analyst', 'AML Analyst')
    supervisor = case.get('supervisor', 'Supervisor')
    team = case.get('team_name', 'AML Transaction Monitoring')
    
    outcomes = {
        "sar_filed": "OUTCOME: SAR FILED. Investigation CONFIRMED suspicious activity. Show: patterns inconsistent with profile, red flags confirmed (cite FFIEC), unsatisfactory customer response. Timeline ends with SAR filed.",
        "closed_no_action": "OUTCOME: CLOSED - NO ACTION. Activity was EXPLAINABLE. Show: legitimate business purpose found, satisfactory documentation received. Timeline ends with FALSE POSITIVE disposition.",
        "escalated": "OUTCOME: ESCALATED TO BSA OFFICER. SERIOUS concerns found. Show: potential criminal patterns, connections to suspicious entities, warrants law enforcement notification. Timeline ends with escalation, potential 314(b).",
        "pending_info": "OUTCOME: PENDING DOCUMENTATION. Investigation INCOMPLETE. Show: questions identified, documentation requested, awaiting customer response. List specific docs needed. Timeline ends with pending status, 15-day deadline."
    }
    
    return f"""You are {analyst}, an investigator on the {team} team at ABC National Bank. Document your investigation of this case.

CASE HEADER:
- Case ID: {case['case_id']}
- Investigator: {analyst}
- Team: {team}
- Supervisor: {supervisor}
- Alert ID: {alert.get('alert_id', 'N/A') if alert else 'N/A'}
- Scenario: {scenario}
- Priority: {str(case.get('priority', 'Medium')).upper()}
- Opened: {case.get('open_date', 'Unknown')}

CUSTOMER: {name} (ID: {customer.get('customer_id')}) | Risk: {str(customer.get('risk_rating', 'Medium')).upper()}
Occupation: {customer.get('occupation', 'N/A')} | Income: ${customer.get('annual_income', 0):,.0f} | PEP: {'Yes' if customer.get('pep_flag') else 'No'}

ALERT: Score {alert_score} | Amount: ${alert_amount:,.2f}

TRANSACTIONS:
{chr(10).join(txn_lines) if txn_lines else '  None'}

{outcomes[outcome]}

Generate timeline as {analyst}: CASE OPENED → PROFILE REVIEW → TRANSACTION ANALYSIS → SCENARIO DEEP DIVE → CUSTOMER OUTREACH → AI ASSISTANT QUERIES (2-3 examples) → DISPOSITION.
Sign off with your name and date at the end."""

# COMMAND ----------

case_dir = f"{VOLUME_PATH}/case_notes"
os.makedirs(case_dir, exist_ok=True)
print("="*70 + "\nGENERATING CASE NOTES\n" + "="*70)

# Check if case notes already exist
existing_case_notes = [f for f in os.listdir(case_dir) if f.startswith("case_") and f.endswith(".txt")] if os.path.exists(case_dir) else []
expected_case_count = len(cases_df)

if len(existing_case_notes) >= expected_case_count:
    print(f"✅ Case notes already exist: {len(existing_case_notes)} files found (expected {expected_case_count})")
    print("   Skipping case notes generation to save time.")
    case_count = len(existing_case_notes)
    case_failed = 0
    case_dist = {"sar_filed": 0, "closed_no_action": 0, "escalated": 0, "pending_info": 0}
else:
    if existing_case_notes:
        print(f"⚠️  Found {len(existing_case_notes)} existing case notes, need {expected_case_count}. Regenerating all...")

    case_dist = {"sar_filed": 0, "closed_no_action": 0, "escalated": 0, "pending_info": 0}
    case_count, case_failed = 0, 0

    for idx, case in cases_df.iterrows():
        cust_row = customers_df[customers_df["customer_id"] == case["customer_id"]]
        if cust_row.empty: continue
        customer = cust_row.iloc[0].to_dict()
        alert_row = alerts_df[alerts_df["alert_id"] == case["alert_id"]]
        alert = alert_row.iloc[0].to_dict() if not alert_row.empty else None
        cust_txns = transactions_df[transactions_df["customer_id"] == case["customer_id"]]

        outcome = random.choices(["sar_filed", "closed_no_action", "escalated", "pending_info"], weights=[30, 40, 15, 15])[0]
        analyst = case.get('assigned_analyst', 'Unknown') if isinstance(case, dict) else case['assigned_analyst']
        print(f"\n[{idx+1}/{len(cases_df)}] Case {case['case_id']} - {outcome.upper()} (Analyst: {analyst})")

        notes = generate_with_llm(build_case_notes_prompt(case.to_dict() if hasattr(case, 'to_dict') else dict(case), customer, alert, cust_txns, outcome))
        if notes:
            with open(f"{case_dir}/case_{case['case_id']:04d}_customer_{case['customer_id']:04d}.txt", 'w') as f: f.write(notes)
            case_count += 1
            case_dist[outcome] += 1
        else: case_failed += 1

    print(f"\nCase Notes: {case_count} generated, {case_failed} failed | Distribution: {case_dist}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 EDD Memos (Varied Outcomes)
# MAGIC
# MAGIC EDD memos are prepared by EDD team analysts and reviewed by their supervisor.

# COMMAND ----------

def build_edd_prompt(customer, transactions, outcome, analyst, supervisor):
    """Build EDD prompt with analyst attribution."""
    name = get_customer_name(customer)
    total = transactions["amount"].sum() if len(transactions) > 0 else 0
    cash_txns = transactions[transactions["transaction_type"].str.contains("cash|deposit", case=False, na=False)]
    wire_txns = transactions[transactions["transaction_type"].str.contains("wire", case=False, na=False)]
    cash_total = cash_txns['amount'].sum() if len(cash_txns) > 0 else 0
    wire_total = wire_txns['amount'].sum() if len(wire_txns) > 0 else 0
    max_txn = transactions['amount'].max() if len(transactions) > 0 else 0
    pep = 'Yes' if customer.get('pep_flag') else 'No'
    
    outcomes = {
        "maintain_high": "RECOMMENDATION: MAINTAIN HIGH RISK. Activity elevated vs income, patterns warrant monitoring, risk factors persist. Next review: 6 months.",
        "downgrade_medium": "RECOMMENDATION: DOWNGRADE TO MEDIUM. Activity now consistent with profile, concerns addressed, documentation satisfactory. Next review: 12 months.",
        "escalate": "RECOMMENDATION: ESCALATE FOR SAR REVIEW. New concerns emerged, adverse media match, warrants SAR consideration. Enhanced monitoring, restrict transactions.",
        "exit_recommend": "RECOMMENDATION: EXIT RELATIONSHIP. Cumulative risk exceeds appetite, multiple red flags, uncooperative, reputational risk. Exit within 30 days, file SAR."
    }
    
    return f"""You are {analyst}, an analyst on the Enhanced Due Diligence (EDD) team at ABC National Bank. Prepare a formal EDD memorandum.

DOCUMENT HEADER:
- Prepared by: {analyst}, EDD Team
- Reviewed by: {supervisor}, EDD Team Lead
- Review Date: 2024-11-15
- Review Type: Periodic High-Risk Customer Review

CUSTOMER: {name} (ID: {customer.get('customer_id')}) | Type: {str(customer.get('customer_type', 'Individual')).title()}
Address: {customer.get('address_city', '')}, {customer.get('address_state', '')} | Since: {customer.get('onboarding_date', 'Unknown')}
Occupation: {customer.get('occupation', 'N/A')} | Income: ${customer.get('annual_income', 0):,.0f} | PEP: {pep}

90-DAY SUMMARY: ${total:,.2f} total ({len(transactions)} txns) | Cash: ${cash_total:,.2f} | Wire: ${wire_total:,.2f} | Max: ${max_txn:,.2f}

{outcomes[outcome]}

Generate EDD memo with:
1) DOCUMENT HEADER (preparer, reviewer, date)
2) CUSTOMER INFO
3) RELATIONSHIP OVERVIEW
4) EDD TRIGGER
5) SOURCE OF FUNDS/WEALTH
6) TRANSACTION ANALYSIS
7) SCREENING RESULTS
8) RISK ASSESSMENT
9) RECOMMENDATION
10) SIGN-OFF (your name, title, date)

Write in formal compliance language as {analyst}."""

# COMMAND ----------

edd_dir = f"{VOLUME_PATH}/edd_memos"
os.makedirs(edd_dir, exist_ok=True)
print("="*70 + "\nGENERATING EDD MEMORANDA\n" + "="*70)

# Check if EDD memos already exist
existing_edds = [f for f in os.listdir(edd_dir) if f.startswith("edd_") and f.endswith(".txt")] if os.path.exists(edd_dir) else []
expected_edd_count = len(high_risk_customers)

if len(existing_edds) >= expected_edd_count:
    print(f"✅ EDD memos already exist: {len(existing_edds)} files found (expected {expected_edd_count})")
    print("   Skipping EDD memo generation to save time.")
    edd_count = len(existing_edds)
    edd_failed = 0
    edd_dist = {"maintain_high": 0, "downgrade_medium": 0, "escalate": 0, "exit_recommend": 0}
else:
    if len(existing_edds) > 0:
        print(f"⚠️  Found {len(existing_edds)} existing EDD memos (expected {expected_edd_count})")
        print("   Regenerating all EDD memos...")

    # Get EDD team analysts and supervisor
    edd_analysts = INVESTIGATION_TEAMS["Enhanced Due Diligence (EDD)"]["analysts"]
    edd_supervisor = INVESTIGATION_TEAMS["Enhanced Due Diligence (EDD)"]["supervisors"][0]

    edd_dist = {"maintain_high": 0, "downgrade_medium": 0, "escalate": 0, "exit_recommend": 0}
    edd_count, edd_failed = 0, 0

    for idx, (_, cust_row) in enumerate(high_risk_customers.iterrows()):
        customer = cust_row.to_dict()
        cust_txns = transactions_df[transactions_df["customer_id"] == customer["customer_id"]]

        outcome = random.choices(["maintain_high", "downgrade_medium", "escalate", "exit_recommend"], weights=[50, 25, 15, 10])[0]
        analyst = random.choice(edd_analysts)
        print(f"\n[{idx+1}/{len(high_risk_customers)}] EDD {get_customer_name(customer)} - {outcome.upper()} (Analyst: {analyst})")

        memo = generate_with_llm(build_edd_prompt(customer, cust_txns, outcome, analyst, edd_supervisor))
        if memo:
            with open(f"{edd_dir}/edd_{customer['customer_id']:04d}.txt", 'w') as f: f.write(memo)
            edd_count += 1
            edd_dist[outcome] += 1
        else: edd_failed += 1

    print(f"\nEDD Memos: {edd_count} generated, {edd_failed} failed | Distribution: {edd_dist}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Adverse Media Screenings (Varied Findings)
# MAGIC
# MAGIC Screenings performed by Sanctions & Watchlist team or EDD team analysts.

# COMMAND ----------

def build_adverse_media_prompt(customer, finding_type, analyst, supervisor):
    """Build adverse media prompt with analyst attribution."""
    name = get_customer_name(customer)
    
    findings = {
        "none": "RESULT: NO FINDINGS. Thorough screening, no relevant matches. Sections: SCREENING DETAILS, SEARCH PARAMETERS, RESULTS (none), SUMMARY (CLEAR).",
        "cleared": "RESULT: FINDING CLEARED. One potential match found but CLEARED (different city, different DOB, or minor resolved matter). Sections: DETAILS, PARAMETERS, RESULTS with Finding #1 (Source, Date, Summary, Relevance LOW/MEDIUM, Analyst Notes why NOT same person, CLEARED), SUMMARY.",
        "flagged": "RESULT: CONFIRMED MATCH - FLAGGED. Match requires escalation (regulatory action, fraud investigation, misconduct lawsuit, or ML association). Sections: DETAILS, PARAMETERS, RESULTS with Finding #1 (Source, Date, Summary, Relevance HIGH, confirmation notes, FLAGGED), RISK ASSESSMENT, RECOMMENDED ACTIONS, SUMMARY (REQUIRES ACTION)."
    }
    
    return f"""You are {analyst}, a screening analyst on the Sanctions & Watchlist Screening team at ABC National Bank. Document an adverse media search.

SCREENING HEADER:
- Screening Analyst: {analyst}
- Reviewed by: {supervisor}
- Screening Date: 2024-11-15
- Screening Type: Periodic Review (High-Risk Customer)

SUBJECT: {name} (ID: {customer.get('customer_id')})
Location: {customer.get('address_city', 'Unknown')}, {customer.get('address_state', 'Unknown')} | Occupation: {customer.get('occupation', 'N/A')}

SOURCES SEARCHED:
- LexisNexis Accurint
- Dow Jones Risk & Compliance
- Google News (5-year lookback)
- SEC EDGAR
- PACER Federal Courts
- State Court Records

{findings[finding_type]}

Include SCREENING HEADER with your name at the top and ANALYST CERTIFICATION with your sign-off at the bottom."""

# COMMAND ----------

media_dir = f"{VOLUME_PATH}/adverse_media"
os.makedirs(media_dir, exist_ok=True)
print("="*70 + "\nGENERATING ADVERSE MEDIA REPORTS\n" + "="*70)

# Check if adverse media reports already exist
existing_media = [f for f in os.listdir(media_dir) if f.startswith("screening_") and f.endswith(".txt")] if os.path.exists(media_dir) else []
expected_media_count = len(high_risk_customers)

if len(existing_media) >= expected_media_count:
    print(f"✅ Adverse media reports already exist: {len(existing_media)} files found (expected {expected_media_count})")
    print("   Skipping adverse media generation to save time.")
    media_count = len(existing_media)
    media_failed = 0
    media_dist = {"none": 0, "cleared": 0, "flagged": 0}
else:
    if len(existing_media) > 0:
        print(f"⚠️  Found {len(existing_media)} existing adverse media reports (expected {expected_media_count})")
        print("   Regenerating all adverse media reports...")

    # Get Sanctions team analysts and supervisor
    sanctions_analysts = INVESTIGATION_TEAMS["Sanctions & Watchlist Screening"]["analysts"]
    sanctions_supervisor = INVESTIGATION_TEAMS["Sanctions & Watchlist Screening"]["supervisors"][0]

    media_dist = {"none": 0, "cleared": 0, "flagged": 0}
    media_count, media_failed = 0, 0

    for idx, (_, cust_row) in enumerate(high_risk_customers.iterrows()):
        customer = cust_row.to_dict()
        finding = random.choices(["none", "cleared", "flagged"], weights=[60, 25, 15])[0]
        analyst = random.choice(sanctions_analysts)
        print(f"\n[{idx+1}/{len(high_risk_customers)}] Media {get_customer_name(customer)} - {finding.upper()} (Analyst: {analyst})")

        report = generate_with_llm(build_adverse_media_prompt(customer, finding, analyst, sanctions_supervisor))
        if report:
            with open(f"{media_dir}/screening_{customer['customer_id']:04d}.txt", 'w') as f: f.write(report)
            media_count += 1
            media_dist[finding] += 1
        else: media_failed += 1

    print(f"\nAdverse Media: {media_count} generated, {media_failed} failed | Distribution: {media_dist}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Correspondence Logs (Varied Interactions)
# MAGIC
# MAGIC Branch staff interactions - these are separate from investigation team analysts.

# COMMAND ----------

def build_correspondence_prompt(customer, transactions, interaction_type):
    name = get_customer_name(customer)
    txn_lines = [f"  - {str(t['transaction_date'])[:10]}: {t['transaction_type'].replace('_',' ').title()}, ${t['amount']:,.2f}" for _, t in transactions.head(5).iterrows()]
    
    # Branch staff (separate from investigators)
    staff_name, staff_role = random.choice(BRANCH_STAFF)
    branch = random.choice(["Main Street Branch", "Downtown Branch", "Westside Branch", "Corporate Center"])
    contact = random.choice(["Branch Visit", "Phone Call", "Secure Message"])
    
    scenarios = {
        "routine_positive": ("Customer updated address / opened CD / added joint holder with docs / set up bill pay", "Pleasant interaction, friendly customer, request completed, positive relationship. ASSESSMENT: Valued customer."),
        "routine_neutral": ("Customer asked about pending txn / wire fees / business accounts / disputed charge", "Standard service, businesslike customer, request handled professionally. ASSESSMENT: No follow-up needed."),
        "minor_concern": ("Customer asked about reporting thresholds during $8K deposit / nervous during wire verification / vague about overseas wire purpose", "Mostly normal but ONE behavior raised questions. Could be legitimate. Note for file, not escalated. ASSESSMENT: Monitor future activity."),
        "significant_concern": ("Customer wanted multiple deposits under $10K asking about reporting / hostile during verification / admitted funds belong to third party", "CLEAR RED FLAGS observed. Potential structuring/ML/fraud. Include specific concerning quotes. Escalated to BSA/Compliance. ASSESSMENT: Potential SAR.")
    }
    
    scenario, guidance = scenarios[interaction_type]
    
    return f"""You are {staff_name}, {staff_role} at ABC National Bank {branch}. Document a customer interaction.

INTERACTION HEADER:
- Staff: {staff_name}, {staff_role}
- Branch: {branch}
- Date: 2024-11-18
- Time: {random.randint(9,11)}:{random.choice(['00','15','30','45'])} AM
- Contact Type: {contact}

CUSTOMER: {name} (ID: {customer.get('customer_id')}) | Risk: {str(customer.get('risk_rating', 'Medium')).upper()}
Occupation: {customer.get('occupation', 'N/A')} | Since: {customer.get('onboarding_date', 'Unknown')}

RECENT ACTIVITY:
{chr(10).join(txn_lines) if txn_lines else '  None'}

SCENARIO: {scenario}
{guidance}

Sections: HEADER (your name, branch, date), SUMMARY (with dialogue), ACTIONS TAKEN, STAFF OBSERVATIONS, PRIOR INTERACTIONS, FOLLOW-UP.
Sign with your name and title at the end."""

# COMMAND ----------

corr_dir = f"{VOLUME_PATH}/correspondence"
os.makedirs(corr_dir, exist_ok=True)
print("="*70 + "\nGENERATING CORRESPONDENCE LOGS\n" + "="*70)

# Check if correspondence logs already exist
existing_corr = [f for f in os.listdir(corr_dir) if f.startswith("corr_") and f.endswith(".txt")] if os.path.exists(corr_dir) else []
expected_corr_count = len(high_risk_customers)

if len(existing_corr) >= expected_corr_count:
    print(f"✅ Correspondence logs already exist: {len(existing_corr)} files found (expected {expected_corr_count})")
    print("   Skipping correspondence generation to save time.")
    corr_count = len(existing_corr)
    corr_failed = 0
    corr_dist = {"routine_positive": 0, "routine_neutral": 0, "minor_concern": 0, "significant_concern": 0}
else:
    if len(existing_corr) > 0:
        print(f"⚠️  Found {len(existing_corr)} existing correspondence logs (expected {expected_corr_count})")
        print("   Regenerating all correspondence logs...")

    corr_dist = {"routine_positive": 0, "routine_neutral": 0, "minor_concern": 0, "significant_concern": 0}
    corr_count, corr_failed = 0, 0

    for idx, (_, cust_row) in enumerate(high_risk_customers.iterrows()):
        customer = cust_row.to_dict()
        cust_txns = transactions_df[transactions_df["customer_id"] == customer["customer_id"]]

        interaction = random.choices(["routine_positive", "routine_neutral", "minor_concern", "significant_concern"], weights=[40, 25, 20, 15])[0]
        print(f"\n[{idx+1}/{len(high_risk_customers)}] Corr {get_customer_name(customer)} - {interaction.upper()}")

        log = generate_with_llm(build_correspondence_prompt(customer, cust_txns, interaction))
        if log:
            with open(f"{corr_dir}/corr_{customer['customer_id']:04d}.txt", 'w') as f: f.write(log)
            corr_count += 1
            corr_dist[interaction] += 1
        else: corr_failed += 1

    print(f"\nCorrespondence: {corr_count} generated, {corr_failed} failed | Distribution: {corr_dist}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("="*70 + "\nKNOWLEDGE BASE COMPLETE\n" + "="*70)
dirs = ["policies_and_regulations/ffiec", "policies_and_regulations/fincen", "policies_and_regulations/ofac", 
        "policies_and_regulations/internal", "sar_narratives", "case_notes", "edd_memos", "adverse_media", "correspondence"]
total = 0
for d in dirs:
    try:
        count = len(os.listdir(f"{VOLUME_PATH}/{d}"))
        total += count
        print(f"{d.split('/')[-1]:20} {count:4} docs")
    except: print(f"{d.split('/')[-1]:20}    0 docs")
print(f"{'TOTAL':20} {total:4} docs\n\nLocation: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyst Attribution Summary
# MAGIC
# MAGIC | Document Type | Analyst Source | Team |
# MAGIC |---------------|----------------|------|
# MAGIC | SAR Narratives | From linked case record | Case team |
# MAGIC | Case Notes | From case record | Case team |
# MAGIC | EDD Memos | Random EDD team analyst | Enhanced Due Diligence |
# MAGIC | Adverse Media | Random Sanctions team analyst | Sanctions & Watchlist |
# MAGIC | Correspondence | Random branch staff | Branch Operations |