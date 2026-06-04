"""
FINS AML Investigation Platform - FastAPI Backend
"""

import os
import logging
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager

from backend import config

# Configure logging first
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import API routers (will create these next)
logger.info("Loading API routers...")
from backend.api.executive import router as executive_router
from backend.api.analyst import router as analyst_router
from backend.api.investigation import router as investigation_router
from backend.api.agent import router as agent_router
from backend.api.sar import router as sar_router
from backend.api.documents import router as documents_router
from backend.api.auth import router as auth_router

logger.info("Loading Databricks graph router...")
try:
    from backend.api.databricks_graph import router as databricks_graph_router
    logger.info("✅ Databricks graph router loaded successfully")
except Exception as e:
    logger.error(f"❌ Failed to load Databricks graph router: {e}")
    databricks_graph_router = None

logger.info("Loading semantic graph router...")
try:
    from backend.api.semantic_graph import router as semantic_graph_router
    logger.info("✅ Semantic graph router loaded successfully")
except Exception as e:
    logger.error(f"❌ Failed to load semantic graph router: {e}")
    semantic_graph_router = None

# Import database service
from backend.services.database import DatabaseService

# Global database service instance
db_service = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and cleanup database connection"""
    global db_service
    try:
        # Initialize database service (uses OAuth M2M via service principal)
        db_service = DatabaseService(
            warehouse_id=config.DATABRICKS_WAREHOUSE_ID,
            hostname=config.DATABRICKS_HOSTNAME
        )
        logger.info("Database service initialized")
        yield
    except Exception as e:
        logger.error(f"Failed to initialize database service: {e}")
        raise
    finally:
        if db_service:
            await db_service.close()
            logger.info("Database service closed")

# Create FastAPI app
app = FastAPI(
    title="FINS AML Platform",
    description="AI-powered Anti-Money Laundering investigation platform",
    version="1.0.0",
    lifespan=lifespan
)

# Include API routers
app.include_router(executive_router, prefix="/api/executive", tags=["executive"])
app.include_router(analyst_router, prefix="/api/analyst", tags=["analyst"])
app.include_router(investigation_router, prefix="/api/investigation", tags=["investigation"])
app.include_router(agent_router, prefix="/api/agent", tags=["agent"])
app.include_router(sar_router, prefix="/api/sar", tags=["sar"])
app.include_router(documents_router, prefix="/api/documents", tags=["documents"])
app.include_router(auth_router, prefix="/api/auth", tags=["auth"])

# Include Databricks graph router if it loaded successfully
if databricks_graph_router:
    app.include_router(databricks_graph_router, prefix="/api/databricks-graph", tags=["graph"])
    logger.info("✅ Databricks graph router registered with app")
else:
    logger.error("❌ Databricks graph router not registered (failed to import)")

# Include semantic graph search router if it loaded successfully
if semantic_graph_router:
    app.include_router(semantic_graph_router, prefix="/api/semantic-graph", tags=["semantic-graph"])
    logger.info("✅ Semantic graph router registered with app")
else:
    logger.error("❌ Semantic graph router not registered (failed to import)")

# Debug routes — exposed only when DEBUG_MODE=true. These endpoints
# return schema and sample-row info that's useful for troubleshooting
# but should not be reachable in production deployments.
if os.getenv("DEBUG_MODE", "false").lower() == "true":
    from backend.api import debug as debug_router
    app.include_router(debug_router.router, prefix="/api/debug", tags=["debug"])
    logger.info("🔧 /api/debug/* endpoints registered (DEBUG_MODE=true)")

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        if db_service:
            await db_service.test_connection()
        return {"status": "healthy", "service": "FINS AML Platform"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")

# Mount static files (React frontend) - only if directory exists
import os
static_dir = "frontend/build/static"
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Serve React app for all other routes
@app.get("/{full_path:path}")
async def serve_react_app(full_path: str):
    """Serve React app for all frontend routes"""
    # API routes should not serve the React app
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="API endpoint not found")

    # Check if index.html exists
    index_path = "frontend/build/index.html"
    if os.path.exists(index_path):
        return FileResponse(index_path)
    else:
        # Return a simple response if no React build
        return {"message": "FINS AML Platform API is running", "docs": "/docs"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)