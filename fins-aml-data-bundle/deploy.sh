#!/usr/bin/env bash
#
# deploy.sh — Deploy the AML data bundle to a Databricks workspace.
#
# Usage:
#   ./deploy.sh <profile> <catalog> <schema> <warehouse_id> [force_rebuild]
#
# Example:
#   ./deploy.sh my-profile financial_services aml 9808cb1bbca5e1bb false
#
# To enable the You.com MCP web-search sub-agent, create the secret first
# (databricks secrets create-scope youcom && databricks secrets put-secret
# youcom api_key), then export these before running:
#   YOUCOM_SECRET_SCOPE=youcom YOUCOM_SECRET_KEY=api_key ./deploy.sh ...
#
# What it does:
#   1. Renders the executive dashboard for your catalog/schema. The dashboard
#      resource deploys from SherlockAML_ExecDash_Processed.lvdash.json, which
#      is gitignored and generated from the parametrized template. It must
#      exist BEFORE `bundle deploy` because DAB embeds the dashboard file as-is
#      and does not substitute the ${catalog}/${schema} placeholders itself
#      (bundle variables are not available to bundle deploy for that file, nor
#      to lifecycle script hooks — verified).
#   2. Runs `databricks bundle deploy` with your variables.
#
# After this, run the pipeline (the command is printed at the end).

set -euo pipefail

PROFILE="${1:?Usage: ./deploy.sh <profile> <catalog> <schema> <warehouse_id> [force_rebuild]}"
CATALOG="${2:?Usage: ./deploy.sh <profile> <catalog> <schema> <warehouse_id> [force_rebuild]}"
SCHEMA="${3:?Usage: ./deploy.sh <profile> <catalog> <schema> <warehouse_id> [force_rebuild]}"
WAREHOUSE_ID="${4:?Usage: ./deploy.sh <profile> <catalog> <schema> <warehouse_id> [force_rebuild]}"
FORCE_REBUILD="${5:-false}"

echo "=== Deploying fins-aml-data-pipeline (catalog=$CATALOG schema=$SCHEMA, profile=$PROFILE) ==="

# Step 1: Render the dashboard for this catalog/schema.
echo ""
echo "Step 1: Rendering executive dashboard for ${CATALOG}.${SCHEMA}..."
python3 process_dashboard_template.py \
  --catalog "$CATALOG" --schema "$SCHEMA" \
  --template SherlockAML_ExecDash_Template.lvdash.json \
  --output SherlockAML_ExecDash_Processed.lvdash.json

# Step 2: Deploy the bundle.
echo ""
echo "Step 2: Deploying the bundle..."
DEPLOY_ARGS=(--profile "$PROFILE"
  --var "catalog=$CATALOG"
  --var "schema=$SCHEMA"
  --var "warehouse_id=$WAREHOUSE_ID"
  --var "force_rebuild=$FORCE_REBUILD")
if [ -n "${YOUCOM_SECRET_SCOPE:-}" ] && [ -n "${YOUCOM_SECRET_KEY:-}" ]; then
    echo "  (enabling You.com MCP from secret ${YOUCOM_SECRET_SCOPE}/${YOUCOM_SECRET_KEY})"
    DEPLOY_ARGS+=(--var "youcom_secret_scope=$YOUCOM_SECRET_SCOPE" --var "youcom_secret_key=$YOUCOM_SECRET_KEY")
fi
databricks bundle deploy "${DEPLOY_ARGS[@]}"

echo ""
echo "=== Bundle deployed. Now run the pipeline (generates data + provisions agents): ==="
echo "  databricks bundle run aml_data_generation_pipeline --profile $PROFILE \\"
echo "    --var catalog=$CATALOG --var schema=$SCHEMA --var warehouse_id=$WAREHOUSE_ID --var force_rebuild=$FORCE_REBUILD"
