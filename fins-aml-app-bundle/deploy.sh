#!/usr/bin/env bash
#
# deploy.sh — Deploy the SherlockAML app bundle to a Databricks workspace.
#
# Usage:
#   ./deploy.sh <target> <profile>
#
# Example:
#   ./deploy.sh fevm-fins-demo fevm-fins-demo
#   ./deploy.sh e2-demo-west e2-demo-west
#
# What it does:
#   1. Validates the bundle configuration
#   2. Runs `databricks bundle deploy` (uploads code to workspace)
#   3. Resolves ${var.xxx} references in app.yaml using target variables
#      from databricks.yml, then uploads the resolved file
#   4. Runs `databricks apps deploy` to start the application
#
# Why step 3 is needed:
#   `databricks bundle deploy` does NOT resolve ${var.xxx} in app.yaml —
#   it only resolves variables in resource definitions (jobs, pipelines)
#   inside databricks.yml. The app.yaml is uploaded as a raw file. This
#   script resolves the variables so the Databricks Apps runtime receives
#   actual values instead of literal "${var.xxx}" strings.

set -euo pipefail

TARGET="${1:?Usage: ./deploy.sh <target> <profile>}"
PROFILE="${2:?Usage: ./deploy.sh <target> <profile>}"

echo "=== Deploying fins-aml-platform to target: $TARGET (profile: $PROFILE) ==="

# Step 1: Validate
echo ""
echo "Step 1: Validating bundle..."
databricks bundle validate -t "$TARGET" --profile "$PROFILE"

# Step 2: Bundle deploy
echo ""
echo "Step 2: Deploying bundle..."
databricks bundle deploy -t "$TARGET" --profile "$PROFILE"

# Step 3: Resolve app.yaml variables
echo ""
echo "Step 3: Resolving app.yaml variables..."

# Get the user email for the workspace path
USER_EMAIL=$(databricks current-user me --profile "$PROFILE" 2>/dev/null | grep -o '"userName":"[^"]*"' | cut -d'"' -f4 || echo "")
if [ -z "$USER_EMAIL" ]; then
    # Fallback: try to get from auth
    USER_EMAIL=$(databricks auth describe --profile "$PROFILE" 2>/dev/null | grep -i "user" | head -1 | awk '{print $NF}' || echo "")
fi

if [ -z "$USER_EMAIL" ]; then
    echo "ERROR: Could not determine user email. Please set USER_EMAIL manually."
    exit 1
fi

REMOTE_PATH="/Workspace/Users/$USER_EMAIL/.bundle/fins-aml-platform/$TARGET/files"
echo "  Remote path: $REMOTE_PATH"

# Extract variable values from databricks.yml for the target
# Uses python for reliable YAML parsing
RESOLVED_YAML=$(python3 - "$TARGET" <<'PYSCRIPT'
import os, sys, yaml, re

target = sys.argv[1]

with open("databricks.yml", "r") as f:
    config = yaml.safe_load(f)

# Optionally merge a local-only override file (gitignored). Lets the
# maintainer keep workspace-specific values out of the public repo.
local_config = {}
if os.path.exists("databricks.local.yml"):
    with open("databricks.local.yml", "r") as f:
        local_config = yaml.safe_load(f) or {}

# Get variable defaults
defaults = {}
for var_name, var_def in config.get("variables", {}).items():
    if isinstance(var_def, dict) and "default" in var_def:
        defaults[var_name] = var_def["default"]

# Get target-specific variable overrides — public file first, then local
public_target_vars = config.get("targets", {}).get(target, {}).get("variables", {}) or {}
local_target_vars = local_config.get("targets", {}).get(target, {}).get("variables", {}) or {}

# Merge: defaults < public target < local override
resolved = {**defaults, **public_target_vars, **local_target_vars}

# Read app.yaml and resolve ${var.xxx} references
with open("app.yaml", "r") as f:
    content = f.read()

def replace_var(match):
    var_name = match.group(1)
    value = resolved.get(var_name, "")
    return str(value)

resolved_content = re.sub(r'\$\{var\.([^}]+)\}', replace_var, content)

print(resolved_content, end="")
PYSCRIPT
)

if [ -z "$RESOLVED_YAML" ]; then
    echo "ERROR: Failed to resolve app.yaml variables."
    exit 1
fi

# Write resolved app.yaml to temp file and upload
TMPFILE=$(mktemp /tmp/app-yaml-resolved.XXXXXX)
echo "$RESOLVED_YAML" > "$TMPFILE"

echo "  Uploading resolved app.yaml..."
databricks workspace import --profile "$PROFILE" \
    "$REMOTE_PATH/app.yaml" --file "$TMPFILE" --format AUTO --overwrite

rm -f "$TMPFILE"
echo "  app.yaml resolved and uploaded."

# Step 4: Deploy the app
echo ""
echo "Step 4: Deploying the app..."
databricks apps deploy fins-aml-platform --profile "$PROFILE" \
    --source-code-path "$REMOTE_PATH"

echo ""
echo "=== Deployment complete! ==="
echo "  Target:  $TARGET"
echo "  Profile: $PROFILE"
echo "  Source:  $REMOTE_PATH"
