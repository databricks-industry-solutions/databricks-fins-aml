"""
Document Management API Router

Provides endpoints for:
- Retrieving existing documentation (EDD, SAR, Media Screening, Case Notes)
- Uploading new documents
- Creating manual notes
- Previewing and downloading documents
"""

import logging
import os
import json
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Form
from fastapi.responses import FileResponse, PlainTextResponse
from pydantic import BaseModel
from typing import List, Optional
import aiofiles
import random

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

# Volume paths
VOLUMES = config.VOLUMES

# Response Models
class DocumentInfo(BaseModel):
    filename: str
    file_path: str
    created_date: Optional[str] = None
    file_size: Optional[int] = None

class DocumentListResponse(BaseModel):
    documents: List[DocumentInfo]
    total_count: int

@router.get("/list/{doc_type}")
async def get_documents(
    doc_type: str,
    customer_id: Optional[str] = None,
    case_id: Optional[str] = None,
    db: DatabaseService = Depends(get_db_service)
):
    """Get list of documents for a specific type and customer/case"""
    try:
        if doc_type not in VOLUMES:
            raise HTTPException(status_code=400, detail=f"Invalid document type: {doc_type}")

        volume_path = VOLUMES[doc_type]
        documents = []

        if doc_type == "edd":
            # EDD files: edd_XXXX.txt where XXXX = customer_id
            if customer_id:
                filename = f"edd_{customer_id.zfill(4)}.txt"
                file_path = os.path.join(volume_path, filename)
                try:
                    # Check if file exists (simulate - in real implementation would check actual file)
                    if os.path.exists(file_path) or customer_id:  # Always return for demo
                        documents.append(DocumentInfo(
                            filename=filename,
                            file_path=file_path,
                            created_date=datetime.now().strftime("%Y-%m-%d"),
                            file_size=1024
                        ))
                except Exception:
                    pass

        elif doc_type == "media":
            # Media screening files: screening_XXXX.txt where XXXX = customer_id
            if customer_id:
                filename = f"screening_{customer_id.zfill(4)}.txt"
                file_path = os.path.join(volume_path, filename)
                try:
                    if os.path.exists(file_path) or customer_id:  # Always return for demo
                        documents.append(DocumentInfo(
                            filename=filename,
                            file_path=file_path,
                            created_date=datetime.now().strftime("%Y-%m-%d"),
                            file_size=2048
                        ))
                except Exception:
                    pass

        elif doc_type == "sar":
            # SAR files: Random 0-5 files (no customer tie)
            try:
                # Simulate random SAR files
                num_files = random.randint(0, 5)
                for i in range(num_files):
                    filename = f"sar_narrative_{str(random.randint(1000, 9999))}.txt"
                    file_path = os.path.join(volume_path, filename)
                    documents.append(DocumentInfo(
                        filename=filename,
                        file_path=file_path,
                        created_date=datetime.now().strftime("%Y-%m-%d"),
                        file_size=3072
                    ))
            except Exception:
                pass

        elif doc_type == "case_notes":
            # Case notes: case_XXXX_customer_YYYY.txt
            if case_id and customer_id:
                filename = f"case_{case_id.zfill(4)}_customer_{customer_id.zfill(4)}.txt"
                file_path = os.path.join(volume_path, filename)
                try:
                    if os.path.exists(file_path) or case_id:  # Always return for demo
                        documents.append(DocumentInfo(
                            filename=filename,
                            file_path=file_path,
                            created_date=datetime.now().strftime("%Y-%m-%d"),
                            file_size=1536
                        ))
                except Exception:
                    pass

        return DocumentListResponse(documents=documents, total_count=len(documents))

    except Exception as e:
        logger.error(f"Failed to get documents: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve documents")

@router.get("/preview/{doc_type}/{filename}")
async def preview_document(doc_type: str, filename: str):
    """Get preview of document content"""
    try:
        if doc_type not in VOLUMES:
            raise HTTPException(status_code=400, detail=f"Invalid document type: {doc_type}")

        # Simulate document content for demo
        if doc_type == "edd":
            content = f"""Enhanced Due Diligence Memo

Customer: {filename.split('_')[1].split('.')[0]}
Date: {datetime.now().strftime('%Y-%m-%d')}

CUSTOMER PROFILE:
The customer is a high-net-worth individual with complex financial relationships.
Regular monitoring required due to elevated risk factors.

RISK ASSESSMENT:
- Source of funds: Verified through business ownership
- Geographic exposure: Multiple jurisdictions
- Transaction patterns: Consistent with business activities

RECOMMENDATIONS:
- Continue enhanced monitoring
- Review annually or upon significant changes
- Flag unusual transaction patterns
"""
        elif doc_type == "media":
            content = f"""Adverse Media Screening Report

Customer ID: {filename.split('_')[1].split('.')[0]}
Screening Date: {datetime.now().strftime('%Y-%m-%d')}

SCREENING RESULTS:
- No adverse media hits found
- Clean regulatory record
- No PEP matches
- No sanctions matches

SOURCES SEARCHED:
- Global news databases
- Regulatory enforcement actions
- Sanctions lists
- PEP databases

CONCLUSION:
Customer poses low reputational risk based on current media screening.
"""
        elif doc_type == "sar":
            content = f"""Suspicious Activity Report Narrative

Report ID: {filename.split('_')[2].split('.')[0]}
Filing Date: {datetime.now().strftime('%Y-%m-%d')}

NARRATIVE:
This Suspicious Activity Report is filed to report potential structuring activity.
The customer conducted multiple cash deposits in amounts designed to evade
currency transaction reporting requirements.

ACTIVITY DESCRIPTION:
Over a period of 10 days, the customer made seven separate cash deposits
totaling $67,950. Each deposit was under $10,000 to avoid CTR filing.

SUSPICIOUS INDICATORS:
- Pattern of transactions just below reporting threshold
- No apparent business justification
- Transactions conducted at multiple branch locations

RECOMMENDATION: File SAR and continue monitoring for similar patterns.
"""
        elif doc_type == "case_notes":
            content = f"""Case Investigation Notes

Case ID: {filename.split('_')[1]}
Customer ID: {filename.split('_')[3].split('.')[0]}
Created: {datetime.now().strftime('%Y-%m-%d %H:%M')}

INVESTIGATION NOTES:

Initial Alert Review:
- Alert triggered on structuring pattern
- Customer has history of large cash deposits
- No prior SAR filings

Follow-up Actions:
- Reviewed 6-month transaction history
- Contacted business banking team
- Verified customer's business activities

Key Findings:
- Customer operates cash-intensive business
- Deposits correlate with daily business revenue
- Pattern appears legitimate upon review

Resolution:
- Alert determined to be false positive
- Customer business model explains cash patterns
- No further action required
"""
        else:
            content = "Document content not available."

        return PlainTextResponse(content)

    except Exception as e:
        logger.error(f"Failed to preview document: {e}")
        raise HTTPException(status_code=500, detail="Failed to preview document")

@router.post("/upload/{doc_type}")
async def upload_document(
    doc_type: str,
    customer_id: str = Form(),
    case_id: Optional[str] = Form(None),
    analyst_name: str = Form(),
    file: UploadFile = File(...)
):
    """Upload a new document"""
    try:
        if doc_type not in VOLUMES:
            raise HTTPException(status_code=400, detail=f"Invalid document type: {doc_type}")

        volume_path = VOLUMES[doc_type]

        # Generate filename with metadata
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_analyst = analyst_name.replace(" ", "_")

        if doc_type == "case_notes" and case_id:
            filename = f"case_{case_id.zfill(4)}_customer_{customer_id.zfill(4)}_{timestamp}_{safe_analyst}_{file.filename}"
        else:
            filename = f"{doc_type}_{customer_id.zfill(4)}_{timestamp}_{safe_analyst}_{file.filename}"

        file_path = os.path.join(volume_path, filename)

        # Save file (simulate for demo)
        logger.info(f"Would save uploaded file to: {file_path}")

        return {"success": True, "filename": filename, "path": file_path}

    except Exception as e:
        logger.error(f"Failed to upload document: {e}")
        raise HTTPException(status_code=500, detail="Failed to upload document")

@router.post("/create-note/{doc_type}")
async def create_manual_note(
    doc_type: str,
    customer_id: str = Form(),
    case_id: Optional[str] = Form(None),
    analyst_name: str = Form(),
    content: str = Form()
):
    """Create a manual text note"""
    try:
        if doc_type not in VOLUMES:
            raise HTTPException(status_code=400, detail=f"Invalid document type: {doc_type}")

        volume_path = VOLUMES[doc_type]

        # Generate filename with metadata
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_analyst = analyst_name.replace(" ", "_")

        if doc_type == "case_notes" and case_id:
            filename = f"case_{case_id.zfill(4)}_customer_{customer_id.zfill(4)}_{timestamp}_{safe_analyst}_note.txt"
        else:
            filename = f"{doc_type}_{customer_id.zfill(4)}_{timestamp}_{safe_analyst}_note.txt"

        file_path = os.path.join(volume_path, filename)

        # Create note content with metadata
        note_content = f"""Manual Note - {doc_type.upper()}

Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Analyst: {analyst_name}
Customer ID: {customer_id}
""" + (f"Case ID: {case_id}\n" if case_id else "") + f"""

CONTENT:
{content}
"""

        # Save note (simulate for demo)
        logger.info(f"Would save manual note to: {file_path}")
        logger.info(f"Content length: {len(note_content)} characters")

        return {"success": True, "filename": filename, "path": file_path, "content_length": len(note_content)}

    except Exception as e:
        logger.error(f"Failed to create manual note: {e}")
        raise HTTPException(status_code=500, detail="Failed to create note")

@router.get("/download/{doc_type}/{filename}")
async def download_document(doc_type: str, filename: str):
    """Download a document file"""
    try:
        if doc_type not in VOLUMES:
            raise HTTPException(status_code=400, detail=f"Invalid document type: {doc_type}")

        volume_path = VOLUMES[doc_type]
        file_path = os.path.join(volume_path, filename)

        # For demo purposes, return the preview content as a downloadable file
        content = await preview_document(doc_type, filename)

        return PlainTextResponse(
            content.body,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        logger.error(f"Failed to download document: {e}")
        raise HTTPException(status_code=500, detail="Failed to download document")