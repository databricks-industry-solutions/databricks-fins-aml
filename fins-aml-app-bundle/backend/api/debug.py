"""
Debug API endpoints for exploring schema
"""

import logging
from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any

from backend.services.database import DatabaseService
from backend import config

router = APIRouter()
logger = logging.getLogger(__name__)

# Dependency to get database service
async def get_db_service() -> DatabaseService:
    from main import db_service
    if not db_service:
        raise HTTPException(status_code=503, detail="Database service unavailable")
    return db_service

@router.get("/schema/alerts")
async def get_alerts_schema(db: DatabaseService = Depends(get_db_service)):
    """Get alerts table schema and sample data"""
    try:
        # Get schema
        schema_query = f"DESCRIBE {config.table('alerts')}"
        schema_result = await db.execute_query(schema_query)

        # Get sample data
        sample_query = f"SELECT * FROM {config.table('alerts')} LIMIT 5"
        sample_result = await db.execute_query(sample_query)

        return {
            "schema": schema_result,
            "sample_data": sample_result[:2] if sample_result else [],
            "column_names": list(sample_result[0].keys()) if sample_result else []
        }
    except Exception as e:
        logger.error(f"Failed to get alerts schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/schema/cases")
async def get_cases_schema(db: DatabaseService = Depends(get_db_service)):
    """Get cases table schema and sample data"""
    try:
        # Get schema
        schema_query = f"DESCRIBE {config.table('cases')}"
        schema_result = await db.execute_query(schema_query)

        # Get sample data
        sample_query = f"SELECT * FROM {config.table('cases')} LIMIT 5"
        sample_result = await db.execute_query(sample_query)

        return {
            "schema": schema_result,
            "sample_data": sample_result[:2] if sample_result else [],
            "column_names": list(sample_result[0].keys()) if sample_result else []
        }
    except Exception as e:
        logger.error(f"Failed to get cases schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/schema/graph_nodes")
async def get_graph_nodes_schema(db: DatabaseService = Depends(get_db_service)):
    """Get graph_nodes table schema and sample data"""
    try:
        # Get schema
        schema_query = f"DESCRIBE {config.table('graph_nodes')}"
        schema_result = await db.execute_query(schema_query)

        # Get sample data
        sample_query = f"SELECT * FROM {config.table('graph_nodes')} LIMIT 5"
        sample_result = await db.execute_query(sample_query)

        return {
            "schema": schema_result,
            "sample_data": sample_result[:2] if sample_result else [],
            "column_names": list(sample_result[0].keys()) if sample_result else []
        }
    except Exception as e:
        logger.error(f"Failed to get graph_nodes schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/schema/graph_edges")
async def get_graph_edges_schema(db: DatabaseService = Depends(get_db_service)):
    """Get graph_edges table schema and sample data"""
    try:
        # Get schema
        schema_query = f"DESCRIBE {config.table('graph_edges')}"
        schema_result = await db.execute_query(schema_query)

        # Get sample data
        sample_query = f"SELECT * FROM {config.table('graph_edges')} LIMIT 5"
        sample_result = await db.execute_query(sample_query)

        return {
            "schema": schema_result,
            "sample_data": sample_result[:2] if sample_result else [],
            "column_names": list(sample_result[0].keys()) if sample_result else []
        }
    except Exception as e:
        logger.error(f"Failed to get graph_edges schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/test/simple_alerts")
async def test_simple_alerts_query(db: DatabaseService = Depends(get_db_service)):
    """Test a simple alerts query to understand structure"""
    try:
        query = f"SELECT * FROM {config.table('alerts')} LIMIT 3"
        result = await db.execute_query(query)

        return {
            "result_count": len(result) if result else 0,
            "columns": list(result[0].keys()) if result else [],
            "data": result
        }
    except Exception as e:
        logger.error(f"Failed simple alerts test: {e}")
        raise HTTPException(status_code=500, detail=str(e))
