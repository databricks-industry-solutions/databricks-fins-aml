# FINS AML Data Bundle

This bundle creates the data pipeline and dashboard for the FINS AML (Anti-Money Laundering) demo application.

## Prerequisites

- Databricks CLI installed and configured
- Unity Catalog enabled workspace
- SQL Warehouse available
- Appropriate permissions to create tables and dashboards

## Deployment Instructions

### 1. Configure Your Databricks CLI Profile

Make sure you have a profile configured for your target workspace:
```bash
databricks configure --profile <your-profile-name>
```

### 2. Find Your SQL Warehouse ID

```bash
databricks warehouses list --profile <your-profile-name>
```
Note the ID of the warehouse you want to use.

### 3. Deploy the Data Bundle

```bash
databricks bundle deploy --profile <your-profile-name> \
  --var="catalog=<your-catalog>" \
  --var="schema=<your-schema>" \
  --var="warehouse_id=<your-warehouse-id>" \
  --var="force_rebuild=true"  # Use false to preserve existing data
```

Example:
```bash
databricks bundle deploy --profile fevm-fins-demo \
  --var="catalog=fins_aml" \
  --var="schema=data_generation" \
  --var="warehouse_id=192fe959f141d27c" \
  --var="force_rebuild=false"
```

### 4. Run the Data Generation Pipeline

```bash
databricks bundle run aml_data_generation_pipeline --profile <your-profile-name>
```

This will:
1. Process the dashboard template with your catalog/schema
2. Generate base AML data tables
3. Perform watchlist screening
4. Create graph features
5. Generate knowledge base documents (if not already present)
6. **Provision the Agent Bricks graph** — 3 Knowledge Assistants, 2 Genie
   Spaces, and the Multi-Agent Supervisor that the app consumes (see below).

## Agent Provisioning

The pipeline's final task (`provision_agents`) creates the full agent graph
that the app's chat/SAR features call. It is **idempotent**: re-running the
pipeline finds existing agents by name and skips them.

### What gets created

| Resource | Source |
|----------|--------|
| Knowledge Assistant: `FIN-AML-case-details` | docs in `knowledge_base/{case_notes, edd_memos, sar_narratives, correspondence}/` |
| Knowledge Assistant: `FIN-AML-policies` | docs in `knowledge_base/policies_and_regulations/` |
| Knowledge Assistant: `FIN-AML-media` | docs in `knowledge_base/adverse_media/` |
| Genie Space: `AML Case360 Executive View` | tables: cases, customers, sar_filings, case_audit_log |
| Genie Space: `AML Alert360 Executive View` | tables: alerts, accounts, customers, transactions, watchlist |
| Multi-Agent Supervisor: `FIN-AML-mas` | wires all of the above + optional You.com MCP |

### Optional: You.com MCP web search

The MAS optionally includes a 6th sub-agent for web search via the You.com
MCP server. The bearer token cannot be carried over from the source workspace,
so **MCP is auto-skipped** unless you provide a Databricks secret holding your
own You.com API key.

To include You.com web search:

```bash
# One-time setup: create a scope and store your You.com API key
databricks secrets create-scope youcom --profile <your-profile-name>
databricks secrets put-secret youcom api_key --profile <your-profile-name>
# (paste your You.com API key when prompted)

# Then deploy with the secret variables pointing at it
databricks bundle deploy --profile <your-profile-name> \
  --var="catalog=<your-catalog>" \
  --var="schema=<your-schema>" \
  --var="warehouse_id=<your-warehouse-id>" \
  --var="force_rebuild=false" \
  --var="youcom_secret_scope=youcom" \
  --var="youcom_secret_key=api_key"
```

If you skip this step the deploy succeeds, but the resulting MAS will have
5 sub-agents (3 KAs + 2 Genies) instead of 6. You can add MCP later by
configuring the secret and re-running the pipeline.

### Validation status

The provisioning script has been validated in dry-run mode against the source
workspace (all 6 sub-agents resolve cleanly to existing resources). The
real-write path (`--apply`, used by the bundle) has not yet been exercised
end-to-end in a fresh workspace. The first real `bundle deploy` into an empty
workspace is effectively the first integration test — please budget time for
that and report any issues.

### Manual re-run

To re-run just the provisioning step without regenerating data:

```bash
# From within fins-aml-data-bundle/
python provision_agents.py \
  --profile <your-profile-name> \
  --catalog <your-catalog> \
  --schema <your-schema> \
  --warehouse-id <your-warehouse-id> \
  --mcp-secret-scope youcom \
  --mcp-secret-key api_key \
  --apply
```

Omit `--apply` for a dry-run that prints what would be created.

## Output for App Bundle Deployment

After successful deployment, provide these values to whoever is deploying the app bundle:

| Parameter | Value | Where to Find |
|-----------|-------|---------------|
| **catalog** | Your chosen catalog | Same as `--var="catalog=..."` |
| **schema** | Your chosen schema | Same as `--var="schema=..."` |
| **warehouse_id** | Your warehouse ID | Same as `--var="warehouse_id=..."` |
| **dashboard_id** | Dashboard ID | After deployment, go to Dashboards page and find "AML Executive Dashboard" |

### Tables Created

The pipeline creates these tables in `<catalog>.<schema>`:
- `customers` - Customer master data
- `transactions` - Transaction records
- `accounts` - Account information
- `alerts` - AML alerts
- `sars` - Suspicious Activity Reports
- `watchlist` - Watchlist screening results
- `graph_features` - Graph analytics features

### Knowledge Base Volume

Unstructured documents are stored in:
```
/Volumes/<catalog>/<schema>/aml_knowledge_base/
```

Including:
- SAR narratives
- Case notes
- EDD memos
- Adverse media reports
- Correspondence logs
- Policy documents

## Efficiency Features

The pipeline includes smart caching:
- **Dashboard Template**: Only regenerates when catalog/schema changes
- **Knowledge Base Documents**: Checks if files exist before regenerating (saves LLM processing time)
- **Force Rebuild**: Set to `false` to preserve existing data and only fill gaps

## Troubleshooting

### Authentication Error
Make sure your Databricks CLI profile is correctly configured and has access to the workspace.

### Warehouse Not Found
Verify the warehouse_id using:
```bash
databricks warehouses list --profile <your-profile-name>
```

### Dashboard Not Appearing
After the job completes, the dashboard should appear in the Dashboards section. If not, check the job logs for errors in the `process_dashboard_template` task.

### Knowledge Base Generation Slow
The knowledge base step uses LLMs to generate documents. If files already exist, it will skip regeneration. To force regeneration, delete the files in the Volume path first.

## Support

For issues or questions, please reach out to the FINS team.