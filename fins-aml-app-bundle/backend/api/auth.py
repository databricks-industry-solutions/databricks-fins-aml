"""
Authentication API endpoints
"""

import os
import logging
import requests
import base64
from fastapi import APIRouter, Request, HTTPException
from typing import Dict, Any

from backend import config

router = APIRouter()
logger = logging.getLogger(__name__)

# Service principal credentials - these are provided by Databricks Apps!
SERVICE_PRINCIPAL_ID = config.DATABRICKS_CLIENT_ID
SERVICE_PRINCIPAL_SECRET = config.DATABRICKS_CLIENT_SECRET

@router.get("/token")
async def get_user_token(request: Request) -> Dict[str, Any]:
    """Get appropriate token for dashboard embedding"""
    try:
        # First, try to get a service principal token for embedding
        if SERVICE_PRINCIPAL_ID and SERVICE_PRINCIPAL_SECRET:
            logger.info("🔐 Using Databricks Apps service principal for embedding...")
            logger.info(f"🔑 Service Principal ID: {SERVICE_PRINCIPAL_ID[:8]}...")  # Log first 8 chars for debugging

            # Step 1: Get base OAuth token from service principal
            token_url = f"{config.DATABRICKS_WORKSPACE_URL}/oidc/v1/token"

            auth_string = f"{SERVICE_PRINCIPAL_ID}:{SERVICE_PRINCIPAL_SECRET}"
            auth_header = base64.b64encode(auth_string.encode()).decode()

            headers = {
                "Authorization": f"Basic {auth_header}",
                "Content-Type": "application/x-www-form-urlencoded"
            }

            data = {
                "grant_type": "client_credentials",
                "scope": "all-apis"
            }

            try:
                response = requests.post(token_url, headers=headers, data=data, timeout=10)

                if response.status_code == 200:
                    base_token = response.json().get("access_token")
                    logger.info("✅ Got base service principal token")

                    # Step 2: Get scoped token for dashboard embedding
                    dashboard_id = config.DASHBOARD_ID
                    tokeninfo_url = f"{config.DATABRICKS_WORKSPACE_URL}/api/2.0/lakeview/dashboards/{dashboard_id}/published/tokeninfo"

                    # Add external viewer ID for tracking
                    tokeninfo_params = {
                        "external_viewer_id": "fins-aml-app-user",
                        "external_value": "default"
                    }

                    tokeninfo_headers = {
                        "Authorization": f"Bearer {base_token}",
                        "Content-Type": "application/json"
                    }

                    logger.info(f"🎯 Getting scoped token for dashboard {dashboard_id}")

                    tokeninfo_response = requests.get(
                        tokeninfo_url,
                        params=tokeninfo_params,
                        headers=tokeninfo_headers,
                        timeout=10
                    )

                    if tokeninfo_response.status_code == 200:
                        token_info = tokeninfo_response.json()
                        logger.info("✅ Got token info for dashboard")

                        # Step 3: Generate scoped token with dashboard claims
                        # Following the exact pattern from documentation
                        import json
                        import urllib.parse

                        # Copy token_info and extract authorization_details
                        params = token_info.copy()
                        authorization_details = params.pop("authorization_details", None)

                        # Build the scoped token request exactly as shown in docs
                        params.update({
                            "grant_type": "client_credentials",
                            "authorization_details": json.dumps(authorization_details) if authorization_details else ""
                        })

                        logger.info(f"📝 Requesting scoped token with params: {list(params.keys())}")

                        # URL encode the parameters
                        scoped_data = urllib.parse.urlencode(params)

                        scoped_response = requests.post(
                            token_url,
                            headers={
                                "Authorization": f"Basic {auth_header}",
                                "Content-Type": "application/x-www-form-urlencoded"
                            },
                            data=scoped_data,
                            timeout=10
                        )

                        if scoped_response.status_code == 200:
                            scoped_token = scoped_response.json().get("access_token")
                            logger.info("✅ Successfully obtained scoped dashboard token")
                            logger.info(f"🎯 Token type: service_principal_scoped, length: {len(scoped_token) if scoped_token else 0}")
                            return {
                                "token": scoped_token,
                                "type": "service_principal_scoped",
                                "authenticated": True
                            }
                        else:
                            logger.error(f"Failed to get scoped token: {scoped_response.status_code} - {scoped_response.text}")
                            # Fall back to base token
                            logger.warning("⚠️ Falling back to base token (may not work for embedding)")
                            return {
                                "token": base_token,
                                "type": "service_principal",
                                "authenticated": True
                            }
                    else:
                        logger.error(f"Failed to get tokeninfo: {tokeninfo_response.status_code} - {tokeninfo_response.text}")
                        # Fall back to base token
                        return {
                            "token": base_token,
                            "type": "service_principal",
                            "authenticated": True
                        }
                else:
                    logger.error(f"Failed to get service principal token: {response.status_code} - {response.text}")
            except Exception as e:
                logger.error(f"Error getting service principal token: {e}")
        else:
            logger.warning("⚠️ DATABRICKS_CLIENT_ID or DATABRICKS_CLIENT_SECRET not found in environment")

        # Fall back to user token if service principal fails
        user_token = request.headers.get('X-Forwarded-Access-Token')

        if user_token:
            logger.info("✅ Found user token in X-Forwarded-Access-Token header (may not work for embedding)")
            return {
                "token": user_token,
                "type": "user",
                "authenticated": True,
                "warning": "User token may not have embedding scopes. Configure SERVICE_PRINCIPAL_SECRET for proper embedding."
            }

        # Fallback to OAuth token or PAT
        try:
            fallback_token = config.get_oauth_token()
            logger.info("⚠️ Using OAuth M2M token as fallback for embedding")
            return {
                "token": fallback_token,
                "type": "oauth_m2m",
                "authenticated": True
            }
        except RuntimeError:
            pass

        logger.error("❌ No authentication token available")
        return {
            "token": None,
            "type": None,
            "authenticated": False
        }

    except Exception as e:
        logger.error(f"Error getting token: {e}")
        raise HTTPException(status_code=500, detail="Failed to get authentication token")

@router.get("/workspace-info")
async def get_workspace_info() -> Dict[str, Any]:
    """Get Databricks workspace information"""
    return {
        "workspace_url": config.DATABRICKS_WORKSPACE_URL,
        "workspace_id": config.DATABRICKS_WORKSPACE_ID,
        "dashboard_id": config.DASHBOARD_ID
    }

@router.get("/debug-env")
async def debug_environment() -> Dict[str, Any]:
    """Debug endpoint to check environment variables"""
    return {
        "has_client_id": bool(os.getenv('DATABRICKS_CLIENT_ID')),
        "has_client_secret": bool(os.getenv('DATABRICKS_CLIENT_SECRET')),
        "client_id_prefix": os.getenv('DATABRICKS_CLIENT_ID', '')[:8] if os.getenv('DATABRICKS_CLIENT_ID') else None,
        "message": "Service principal credentials are available" if (os.getenv('DATABRICKS_CLIENT_ID') and os.getenv('DATABRICKS_CLIENT_SECRET')) else "Missing service principal credentials"
    }