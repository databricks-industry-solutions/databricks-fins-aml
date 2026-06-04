# FINS AML App Bundle — Deployment Guide

This document describes how to deploy the SherlockAML app to any Databricks workspace.

---

## Data Bundle + App Bundle: What Must Be Synced

This repo contains two independent Databricks Asset Bundles. The **data bundle** (`fins-aml-data-bundle`) generates the AML tables and dashboard. The **app bundle** (`fins-aml-app-bundle`) deploys the SherlockAML application that reads from those tables. They are deployed separately, but **four variables must match** between them:

| Variable | Data Bundle (`fins-aml-data-pipeline`) | App Bundle (`fins-aml-platform`) | Why |
|---|---|---|---|
| **`catalog`** | Creates tables in this catalog | Reads tables from this catalog | App queries the tables the pipeline generates |
| **`schema`** | Creates tables in this schema | Reads tables from this schema | Same |
| **`warehouse_id`** | Runs pipeline SQL on this warehouse | Connects to this warehouse for live queries | Both must use the same warehouse for consistent access |
| **`dashboard_id`** | Creates/updates this dashboard | Embeds this dashboard in the Executive Overview | App embeds the dashboard the pipeline creates |

**Deploy order**: Data bundle first (creates tables + dashboard), then app bundle (needs the tables to exist and the dashboard ID to reference).

**Everything else is independent**: the app bundle has additional variables (`mas_endpoint_url`, `databricks_hostname`, `workspace_id`, etc.) that have no counterpart in the data bundle.

---

## Prerequisites

Before deploying, gather these values from your target workspace:

| Value | Where to find it |
|---|---|
| **Workspace hostname** | URL bar, e.g. `my-workspace.cloud.databricks.com` (no `https://` prefix) |
| **Workspace ID** | URL `?o=` parameter, or Workspace Settings |
| **SQL Warehouse ID** | SQL Warehouses page -> copy ID |
| **Dashboard ID** | Open dashboard -> ID in URL (`/dashboards/<id>`), or from data bundle output |
| **MAS endpoint URL** | Serving Endpoints -> your MAS endpoint -> full invocation URL |
| **Unity Catalog / Schema** | The catalog and schema where AML tables live (must match data bundle) |
| **Databricks CLI profile** | `~/.databrickscfg` profile name for the target workspace |

---

## Architecture

### Authentication

The app uses **OAuth M2M (service principal)** for all Databricks API calls:

- **SQL Warehouse queries** -- Uses `databricks-sdk` credential provider (auto-refreshing tokens)
- **Serving endpoint calls** (MAS agent, SAR generation) -- Uses OAuth token via `config.get_oauth_token()`
- **Dashboard embedding** -- Uses service principal scoped token flow

The Databricks Apps runtime automatically injects `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` for the app's service principal. **No PAT token is required.**

A PAT (`DATABRICKS_TOKEN`) is supported as an optional fallback for local development only.

### How app.yaml Parameterization Works

The `app.yaml` file contains `${var.xxx}` bundle variable references for all workspace-specific values. **However, `databricks bundle deploy` does not resolve these variables in app.yaml** -- it only resolves them in resource definitions (jobs, pipelines) inside `databricks.yml`. The `app.yaml` is uploaded as a raw file.

This means deploying to a new workspace requires a **post-deploy step** to resolve the variables before starting the app. The `deploy.sh` script (included in this bundle) handles this automatically.

### DATABRICKS_HOST vs DATABRICKS_HOSTNAME

The Databricks Apps runtime auto-injects `DATABRICKS_HOST` (with `https://` prefix, e.g. `https://my-workspace.cloud.databricks.com`). The app also sets `DATABRICKS_HOSTNAME` (without prefix, e.g. `my-workspace.cloud.databricks.com`) via the bundle variable. Both are used:

- `DATABRICKS_HOST` -- Used by the Databricks SDK for OAuth token acquisition
- `DATABRICKS_HOSTNAME` -- Used by the app code for SQL connector hostname and API URL construction

When adding a target to `databricks.yml`, set `databricks_hostname` to the bare hostname (no `https://`).

---

## Deploying to a New Workspace

### Step 1: Add a target to `databricks.yml`

Add a new block under `targets:`:

```yaml
targets:
  my-new-workspace:
    workspace:
      host: https://my-workspace.cloud.databricks.com
    variables:
      databricks_hostname: "my-workspace.cloud.databricks.com"
      workspace_id: "1234567890"
      warehouse_id: "abc123def456"
      catalog: "fins_aml"
      schema: "data_generation"
      mas_endpoint_url: "https://my-workspace.cloud.databricks.com/serving-endpoints/my-mas/invocations"
      dashboard_id: "your-dashboard-id"
```

Variables with defaults (`catalog`, `schema`, `mas_endpoint_url`, `dashboard_id`) can be omitted if the defaults are acceptable.

### Step 2: Deploy using the deploy script

```bash
./deploy.sh my-new-workspace YOUR_PROFILE
```

This script does three things in order:
1. Validates and runs `databricks bundle deploy` (uploads code to workspace)
2. Resolves `${var.xxx}` references in `app.yaml` using the target's variable values from `databricks.yml`, then uploads the resolved file
3. Runs `databricks apps deploy` to start the application

**Manual alternative** (if you prefer not to use the script):

```bash
# 1. Deploy the bundle
databricks bundle validate -t my-new-workspace --profile YOUR_PROFILE
databricks bundle deploy -t my-new-workspace --profile YOUR_PROFILE

# 2. Manually resolve app.yaml: copy app.yaml, replace each ${var.xxx}
#    with the actual value from your target block, then upload:
databricks workspace import --profile YOUR_PROFILE --overwrite \
  /Workspace/Users/YOUR_EMAIL/.bundle/fins-aml-platform/my-new-workspace/files/app.yaml \
  --file ./app.yaml.resolved --format AUTO

# 3. Deploy the app
databricks apps deploy fins-aml-platform --profile YOUR_PROFILE \
  --source-code-path /Workspace/Users/YOUR_EMAIL/.bundle/fins-aml-platform/my-new-workspace/files
```

### Step 3: Configure app resources

After the first deploy, open the app in the Databricks UI (**Apps -> fins-aml-platform -> Resources**) and configure:

| Resource | Type | What to set |
|---|---|---|
| `sql_warehouse` | SQL Warehouse | Select your SQL warehouse (provides `DATABRICKS_WAREHOUSE_ID`) |
| `serving.serving-endpoints` | Serving Endpoint | Maps to your MAS agent endpoint (grants `CAN_QUERY`) |

No PAT or external database secrets are needed -- the app authenticates via the auto-injected service principal, and the graph visualization uses native Databricks tables.

### Step 4: Grant permissions

The app's service principal needs:

1. **CAN USE** on the SQL warehouse
2. **CAN QUERY** on the MAS serving endpoint
3. **CAN RUN** on the published Lakeview dashboard (for embedding)
4. **CAN USE** on the SQL warehouse that the dashboard queries (may be the same warehouse)
5. **SELECT** on the Unity Catalog tables (`catalog.schema.*`)

---

## What Degrades When Optional Variables Are Empty

If you omit optional variables (those with `default: ""`), the corresponding features are disabled gracefully:

| Variable | Default | Feature affected | What happens |
|---|---|---|---|
| `mas_endpoint_url` | `""` | AI Investigation Chat, SAR Narrative Generation | Chat returns an error; SAR generation falls back to a template narrative |
| `dashboard_id` | `""` | Executive Overview (embedded dashboard) | Executive tab shows a loading state; Alert Investigation tab works normally |

---

## Files Overview

| File | Purpose | Edit needed for new workspace? |
|---|---|---|
| `databricks.yml` | Bundle config with targets and variables | **Yes** -- add a target block |
| `deploy.sh` | Automated deploy script (resolve + deploy) | No |
| `app.yaml` | App runtime config with `${var.xxx}` references | No (resolved by deploy.sh) |
| `backend/config.py` | Python config with OAuth M2M token management | No |
| `backend/api/databricks_graph.py` | Native Databricks graph visualization | No |
| `backend/api/agent.py` | MAS agent chat (OAuth auth) | No |
| `backend/api/sar.py` | SAR narrative generation (OAuth auth) | No |
| `backend/api/auth.py` | Dashboard embedding auth flow | No |
| `backend/services/database.py` | SQL warehouse connection (OAuth credential provider) | No |
| `main.py` | FastAPI app entrypoint | No |
| `requirements.txt` | Python dependencies (includes `databricks-sdk`) | No |
| `frontend/build/index.html` | React frontend (fetches config from API at runtime) | No |
| `.env.example` | Example env vars for local development | No |
| `_stashed_neo4j/` | Archived Neo4j integration (see README inside) | No |

---

## Environment Variables

### Auto-injected by Databricks Apps runtime
| Variable | Description |
|---|---|
| `DATABRICKS_HOST` | Workspace URL (with `https://` prefix) |
| `DATABRICKS_CLIENT_ID` | Service principal OAuth client ID |
| `DATABRICKS_CLIENT_SECRET` | Service principal OAuth secret |

### Resolved from bundle variables (via `app.yaml`)
| Variable | Bundle variable | Default |
|---|---|---|
| `DATABRICKS_HOSTNAME` | `databricks_hostname` | (required) |
| `DATABRICKS_WORKSPACE_ID` | `workspace_id` | (required) |
| `DATABRICKS_WAREHOUSE_ID` | (auto from sql_warehouse resource) | -- |
| `DATABRICKS_CATALOG` | `catalog` | `fins_aml` |
| `DATABRICKS_SCHEMA` | `schema` | `data_generation` |
| `DATABRICKS_DASHBOARD_ID` | `dashboard_id` | `""` |
| `MAS_ENDPOINT_URL` | `mas_endpoint_url` | `""` |

---

## Local Development

For local development without the Databricks Apps runtime:

1. Copy `.env.example` to `.env`
2. Fill in `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` (or `DATABRICKS_TOKEN` as fallback)
3. Fill in all workspace-specific values
4. Run: `uvicorn main:app --host 0.0.0.0 --port 8000`

---

## Dashboard Setup

For the embedded executive dashboard to work:

1. **Create and publish** a Lakeview dashboard with the AML queries (or deploy the data bundle first, which creates it)
2. **Publish with embed credentials** -- The dashboard must be published with `embed_credentials: true` for the external embedding flow to work. The data bundle does this automatically. If publishing manually: Dashboard -> Share -> Publish -> enable "Embed credentials"
3. **Share** the dashboard with the app's service principal (**CAN RUN**)
4. **Grant warehouse access** -- The service principal also needs **CAN USE** on the SQL warehouse that the dashboard's queries run against
5. **Set** the `dashboard_id` variable in your target block
6. The dashboard theme is controlled by its `uiSettings.theme` -- the embed path renders the `light` mode colors, so ensure those are set to your desired palette
