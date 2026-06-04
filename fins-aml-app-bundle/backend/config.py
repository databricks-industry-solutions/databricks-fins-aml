"""
Centralized configuration module for FINS AML Platform.

All environment-specific values are read from environment variables
with sensible defaults (current dev workspace values).

Authentication: The app uses OAuth M2M (service principal) via the
DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET environment variables
that the Databricks Apps runtime injects automatically.  A PAT
(DATABRICKS_TOKEN) is kept as an optional fallback for local development.
"""

import os
import logging
import threading
import time

_logger = logging.getLogger(__name__)

# Databricks Workspace
DATABRICKS_HOSTNAME = os.getenv("DATABRICKS_HOSTNAME", "")
DATABRICKS_WORKSPACE_URL = f"https://{DATABRICKS_HOSTNAME}"
DATABRICKS_WORKSPACE_ID = os.getenv("DATABRICKS_WORKSPACE_ID", "")

# Auth — Service Principal (preferred) and PAT (fallback)
DATABRICKS_CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID")
DATABRICKS_CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")  # optional fallback
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "dummy_warehouse")

# Lakebase: optional Postgres-backed graph reads. When USE_LAKEBASE=true the
# customer graph endpoint reads from Postgres instead of the SQL warehouse.
USE_LAKEBASE = os.getenv("USE_LAKEBASE", "false").lower() == "true"

# ---------------------------------------------------------------------------
# OAuth M2M token management
# ---------------------------------------------------------------------------
_oauth_token: str | None = None
_oauth_token_expiry: float = 0
_oauth_lock = threading.Lock()


def _fetch_oauth_token() -> str:
    """Fetch a fresh OAuth token from the service principal credentials."""
    import requests

    token_url = f"{DATABRICKS_WORKSPACE_URL}/oidc/v1/token"
    response = requests.post(
        token_url,
        auth=(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET),
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        timeout=15,
    )
    response.raise_for_status()
    data = response.json()
    return data["access_token"]


def get_oauth_token() -> str:
    """Return a cached OAuth token, refreshing if expired or about to expire.

    Falls back to DATABRICKS_TOKEN (PAT) if service principal credentials
    are not available (e.g. local development).
    """
    global _oauth_token, _oauth_token_expiry

    if not DATABRICKS_CLIENT_ID or not DATABRICKS_CLIENT_SECRET:
        if DATABRICKS_TOKEN:
            _logger.debug("No SP credentials — using PAT fallback")
            return DATABRICKS_TOKEN
        raise RuntimeError(
            "No authentication available: set DATABRICKS_CLIENT_ID + "
            "DATABRICKS_CLIENT_SECRET, or DATABRICKS_TOKEN as fallback."
        )

    # Refresh if token is missing or within 5 min of expiry
    with _oauth_lock:
        if _oauth_token is None or time.time() > (_oauth_token_expiry - 300):
            _logger.info("🔑 Fetching new OAuth M2M token…")
            _oauth_token = _fetch_oauth_token()
            # OAuth tokens are valid for 1 hour
            _oauth_token_expiry = time.time() + 3600
            _logger.info("✅ OAuth token acquired")

    return _oauth_token


def get_sql_credentials_provider():
    """Return a credentials_provider callable for databricks-sql-connector.

    The SQL connector expects a *no-arg function* that, when called,
    returns the result of ``oauth_service_principal(config)``.

    Falls back to None when only a PAT is available (caller should then
    use access_token instead).
    """
    if not DATABRICKS_CLIENT_ID or not DATABRICKS_CLIENT_SECRET:
        return None

    from databricks.sdk.core import Config, oauth_service_principal

    def _provider():
        cfg = Config(
            host=DATABRICKS_WORKSPACE_URL,
            client_id=DATABRICKS_CLIENT_ID,
            client_secret=DATABRICKS_CLIENT_SECRET,
        )
        return oauth_service_principal(cfg)

    return _provider


# MAS Agent Endpoint
MAS_ENDPOINT_URL = os.getenv("MAS_ENDPOINT_URL", "")

# Dashboard Embedding
DASHBOARD_ID = os.getenv("DATABRICKS_DASHBOARD_ID", "")

# Catalog / Schema
CATALOG = os.getenv("DATABRICKS_CATALOG", "fins_aml")
SCHEMA = os.getenv("DATABRICKS_SCHEMA", "data_generation")


def table(name: str) -> str:
    """Return fully-qualified table name."""
    return f"{CATALOG}.{SCHEMA}.{name}"


# Volume base path
VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/knowledge_base"
VOLUMES = {
    "edd": f"{VOLUME_BASE}/edd_memos/",
    "media": f"{VOLUME_BASE}/adverse_media/",
    "sar": f"{VOLUME_BASE}/sar_narratives/",
    "case_notes": f"{VOLUME_BASE}/case_notes/",
}
