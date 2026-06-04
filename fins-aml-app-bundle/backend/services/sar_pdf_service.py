"""
SAR PDF Generation Service

Generates FinCEN-compliant Suspicious Activity Report PDFs with a simplified
3-section narrative structure:
- Summary (intro + who + what)
- Activity Details (when + where + how)
- Conclusion (why + recommendation)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import List, Dict, Any, Optional

from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT, TA_CENTER
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    HRFlowable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


@dataclass
class SARData:
    """Complete SAR filing data structure."""
    # Filing Information
    filing_institution: str = "First National Bank"
    filing_date: Optional[datetime] = None
    sar_reference_number: Optional[str] = None
    
    # Subject Information
    subject_name: str = ""
    subject_type: str = "Individual"
    subject_account_number: str = ""
    subject_address: str = ""
    subject_occupation: str = ""
    
    # Activity Information
    activity_date_range_start: str = ""
    activity_date_range_end: str = ""
    total_amount: float = 0.0
    transaction_count: int = 0
    scenario_type: str = ""
    scenario_name: str = ""
    alert_score: int = 0
    
    # Transaction Details
    transactions: List[Dict[str, Any]] = None
    
    # Narrative Sections (simplified 3-section structure)
    narrative_summary: str = ""
    narrative_activity_details: str = ""
    narrative_conclusion: str = ""
    
    # AI Analysis
    ai_confidence_score: int = 0
    ai_recommendation: str = ""
    ai_rationale: str = ""
    
    # Filing Details
    analyst_name: str = ""
    analyst_team: str = ""
    supervisor_name: str = ""
    
    def __post_init__(self):
        if self.transactions is None:
            self.transactions = []
        if self.filing_date is None:
            self.filing_date = datetime.now()


# Color scheme matching SherlockAML branding
_PRIMARY_COLOR = colors.HexColor('#0f172a')
_ACCENT_COLOR = colors.HexColor('#6366f1')
_BODY_TEXT_COLOR = colors.HexColor('#1e293b')
_META_TEXT_COLOR = colors.HexColor('#64748b')
_WARNING_COLOR = colors.HexColor('#f43f5e')
_SUCCESS_COLOR = colors.HexColor('#10b981')


def generate_sar_pdf(sar_data: SARData) -> bytes:
    """Generate a FinCEN-compliant SAR PDF document."""
    styles = _initialize_styles()
    
    buffer = BytesIO()
    document = SimpleDocTemplate(
        buffer,
        pagesize=LETTER,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=1.0 * inch,
        bottomMargin=0.75 * inch,
    )
    
    story = _build_sar_story(sar_data, styles)
    
    document.build(
        story,
        onFirstPage=lambda canvas, doc: _draw_sar_header(canvas, doc, sar_data),
        onLaterPages=lambda canvas, doc: _draw_sar_header(canvas, doc, sar_data),
    )
    
    payload = buffer.getvalue()
    buffer.close()
    return payload


def _initialize_styles() -> dict:
    """Create paragraph styles for SAR document."""
    base = getSampleStyleSheet()
    
    return {
        'title': ParagraphStyle(
            'SARTitle',
            parent=base['Heading1'],
            fontName='Helvetica-Bold',
            fontSize=18,
            leading=22,
            textColor=_PRIMARY_COLOR,
            alignment=TA_CENTER,
            spaceAfter=6,
        ),
        'subtitle': ParagraphStyle(
            'SARSubtitle',
            parent=base['Normal'],
            fontName='Helvetica',
            fontSize=10,
            leading=14,
            textColor=_META_TEXT_COLOR,
            alignment=TA_CENTER,
            spaceAfter=20,
        ),
        'section_header': ParagraphStyle(
            'SARSectionHeader',
            parent=base['Heading2'],
            fontName='Helvetica-Bold',
            fontSize=12,
            leading=16,
            textColor=_PRIMARY_COLOR,
            spaceBefore=16,
            spaceAfter=8,
        ),
        'subsection_header': ParagraphStyle(
            'SARSubsectionHeader',
            parent=base['Heading3'],
            fontName='Helvetica-Bold',
            fontSize=10,
            leading=13,
            textColor=_ACCENT_COLOR,
            spaceBefore=10,
            spaceAfter=4,
        ),
        'body': ParagraphStyle(
            'SARBody',
            parent=base['BodyText'],
            fontName='Helvetica',
            fontSize=10,
            leading=14,
            textColor=_BODY_TEXT_COLOR,
            alignment=TA_JUSTIFY,
            spaceAfter=8,
        ),
        'meta': ParagraphStyle(
            'SARMeta',
            parent=base['Normal'],
            fontName='Helvetica',
            fontSize=9,
            leading=12,
            textColor=_META_TEXT_COLOR,
            spaceAfter=4,
        ),
        'warning': ParagraphStyle(
            'SARWarning',
            parent=base['Normal'],
            fontName='Helvetica-Bold',
            fontSize=9,
            leading=12,
            textColor=_WARNING_COLOR,
            alignment=TA_CENTER,
        ),
        'label': ParagraphStyle(
            'SARLabel',
            parent=base['Normal'],
            fontName='Helvetica-Bold',
            fontSize=9,
            leading=12,
            textColor=_META_TEXT_COLOR,
        ),
    }


def _build_sar_story(sar_data: SARData, styles: dict) -> list:
    """Build the complete SAR document story."""
    story = []
    
    # Title
    story.append(Paragraph('SUSPICIOUS ACTIVITY REPORT', styles['title']))
    story.append(Paragraph(
        f'Filing Institution: {sar_data.filing_institution}',
        styles['subtitle']
    ))
    story.append(Spacer(1, 10))
    
    # Confidentiality Warning
    story.append(Paragraph(
        'CONFIDENTIAL - FOR LAW ENFORCEMENT USE ONLY',
        styles['warning']
    ))
    story.append(Paragraph(
        'This document contains sensitive information protected under 31 U.S.C. § 5318(g)(2)',
        styles['meta']
    ))
    story.append(Spacer(1, 12))
    
    # Horizontal rule
    story.append(HRFlowable(
        width='100%',
        thickness=2,
        lineCap='round',
        color=_ACCENT_COLOR,
        spaceAfter=16,
    ))
    
    # Filing Information
    story.append(Paragraph('FILING INFORMATION', styles['section_header']))
    story.append(_build_info_table([
        ['SAR Reference Number:', sar_data.sar_reference_number or 'To be assigned'],
        ['Filing Date:', sar_data.filing_date.strftime('%m/%d/%Y') if sar_data.filing_date else ''],
        ['Filing Institution:', sar_data.filing_institution],
        ['Alert ID:', sar_data.scenario_type or ''],
    ]))
    story.append(Spacer(1, 12))
    
    # Subject Information
    story.append(Paragraph('SUBJECT INFORMATION', styles['section_header']))
    story.append(_build_info_table([
        ['Subject Name:', sar_data.subject_name],
        ['Subject Type:', sar_data.subject_type],
        ['Account Number:', sar_data.subject_account_number or 'On file'],
        ['Occupation:', sar_data.subject_occupation or 'N/A'],
    ]))
    story.append(Spacer(1, 12))
    
    # Suspicious Activity Information
    story.append(Paragraph('SUSPICIOUS ACTIVITY INFORMATION', styles['section_header']))
    amount_str = f"${sar_data.total_amount:,.2f}" if sar_data.total_amount else "Unknown"
    story.append(_build_info_table([
        ['Activity Type:', sar_data.scenario_name],
        ['Activity Date Range:', f'{sar_data.activity_date_range_start} to {sar_data.activity_date_range_end}'],
        ['Total Amount Involved:', amount_str],
        ['Number of Transactions:', str(sar_data.transaction_count)],
        ['Alert Score:', f'{sar_data.alert_score}/100'],
    ]))
    story.append(Spacer(1, 12))
    
    # AI Analysis
    story.append(Paragraph('AI-ASSISTED ANALYSIS', styles['section_header']))
    confidence_color = _SUCCESS_COLOR if sar_data.ai_confidence_score >= 80 else _WARNING_COLOR
    ai_table = Table([
        ['AI Confidence Score:', f'{sar_data.ai_confidence_score}%'],
        ['AI Recommendation:', sar_data.ai_recommendation or 'SAR Filing Recommended'],
    ], colWidths=[2.0 * inch, 4.5 * inch])
    ai_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('TEXTCOLOR', (0, 0), (0, -1), _META_TEXT_COLOR),
        ('TEXTCOLOR', (1, 0), (1, 0), confidence_color),
        ('TEXTCOLOR', (1, 1), (1, -1), _BODY_TEXT_COLOR),
        ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
        ('ALIGN', (1, 0), (1, -1), 'LEFT'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
    ]))
    story.append(ai_table)
    
    if sar_data.ai_rationale:
        story.append(Spacer(1, 8))
        story.append(Paragraph('AI Analysis Rationale:', styles['label']))
        story.append(Paragraph(sar_data.ai_rationale, styles['body']))
    story.append(Spacer(1, 12))
    
    # SAR Narrative (3 sections)
    story.append(Paragraph('SAR NARRATIVE', styles['section_header']))
    story.append(Paragraph('Prepared following FinCEN SAR Narrative Guidance', styles['meta']))
    story.append(Spacer(1, 8))
    
    if sar_data.narrative_summary:
        story.append(Paragraph('Summary', styles['subsection_header']))
        story.append(Paragraph(sar_data.narrative_summary, styles['body']))
    
    if sar_data.narrative_activity_details:
        story.append(Paragraph('Activity Details', styles['subsection_header']))
        story.append(Paragraph(sar_data.narrative_activity_details, styles['body']))
    
    if sar_data.narrative_conclusion:
        story.append(Paragraph('Conclusion', styles['subsection_header']))
        story.append(Paragraph(sar_data.narrative_conclusion, styles['body']))
    
    story.append(Spacer(1, 16))
    
    # Transaction Summary
    if sar_data.transactions:
        story.append(Paragraph('TRANSACTION SUMMARY', styles['section_header']))
        story.append(_build_transaction_table(sar_data.transactions))
        story.append(Spacer(1, 12))
    
    # Filing Certification
    story.append(HRFlowable(
        width='100%',
        thickness=1,
        lineCap='round',
        color=_META_TEXT_COLOR,
        spaceBefore=16,
        spaceAfter=12,
    ))
    
    story.append(Paragraph('FILING CERTIFICATION', styles['section_header']))
    story.append(Paragraph(
        "I certify that the information contained in this Suspicious Activity Report is accurate "
        "and complete to the best of my knowledge and belief.",
        styles['body']
    ))
    story.append(Spacer(1, 16))
    
    sig_table = Table([
        ['Prepared By:', sar_data.analyst_name or '________________________'],
        ['Team:', sar_data.analyst_team or ''],
        ['Reviewed By:', sar_data.supervisor_name or '________________________'],
        ['Date:', sar_data.filing_date.strftime('%m/%d/%Y') if sar_data.filing_date else ''],
    ], colWidths=[1.5 * inch, 3.0 * inch])
    sig_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('TEXTCOLOR', (0, 0), (-1, -1), _BODY_TEXT_COLOR),
        ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
    ]))
    story.append(sig_table)
    
    return story


def _build_info_table(data: list) -> Table:
    """Build a standard info table."""
    table = Table(data, colWidths=[2.0 * inch, 4.5 * inch])
    table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('TEXTCOLOR', (0, 0), (0, -1), _META_TEXT_COLOR),
        ('TEXTCOLOR', (1, 0), (1, -1), _BODY_TEXT_COLOR),
        ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
        ('ALIGN', (1, 0), (1, -1), 'LEFT'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 2),
    ]))
    return table


def _build_transaction_table(transactions: List[Dict]) -> Table:
    """Build transaction summary table."""
    header = ['Date', 'Type', 'Amount', 'Channel', 'Flags']
    data = [header]
    
    for txn in transactions[:20]:
        data.append([
            txn.get('date', '')[:10] if txn.get('date') else '',
            txn.get('type', ''),
            f"${txn.get('amount', 0):,.2f}",
            txn.get('channel', ''),
            ', '.join(txn.get('flags', [])) if txn.get('flags') else '',
        ])
    
    if len(transactions) > 20:
        data.append(['', '', f'... and {len(transactions) - 20} more', '', ''])
    
    table = Table(data, colWidths=[1.1 * inch, 1.0 * inch, 1.2 * inch, 1.2 * inch, 2.0 * inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), _ACCENT_COLOR),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('TEXTCOLOR', (0, 1), (-1, -1), _BODY_TEXT_COLOR),
        ('ALIGN', (2, 1), (2, -1), 'RIGHT'),
        ('GRID', (0, 0), (-1, -1), 0.5, _META_TEXT_COLOR),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8fafc')]),
    ]))
    return table


def _draw_sar_header(canvas, document, sar_data: SARData) -> None:
    """Draw the page header and footer."""
    canvas.saveState()
    
    width, height = document.pagesize
    header_y = height - 0.6 * inch
    
    canvas.setFont('Helvetica-Bold', 10)
    canvas.setFillColor(_PRIMARY_COLOR)
    canvas.drawString(document.leftMargin, header_y, 'SherlockAML')
    
    canvas.setFont('Helvetica', 8)
    canvas.setFillColor(_META_TEXT_COLOR)
    ref_text = f"SAR Ref: {sar_data.sar_reference_number or 'Pending'}"
    canvas.drawRightString(width - document.rightMargin, header_y, ref_text)
    
    canvas.setStrokeColor(_ACCENT_COLOR)
    canvas.setLineWidth(1)
    canvas.line(document.leftMargin, header_y - 8, width - document.rightMargin, header_y - 8)
    
    footer_y = 0.5 * inch
    canvas.setFont('Helvetica', 7)
    canvas.setFillColor(_WARNING_COLOR)
    canvas.drawString(document.leftMargin, footer_y, 'CONFIDENTIAL - SAR')
    
    canvas.setFillColor(_META_TEXT_COLOR)
    canvas.drawCentredString(width / 2, footer_y, f'Page {canvas.getPageNumber()}')
    
    date_str = sar_data.filing_date.strftime('%m/%d/%Y') if sar_data.filing_date else ''
    canvas.drawRightString(width - document.rightMargin, footer_y, date_str)
    
    canvas.restoreState()


def generate_sar_from_alert(
    alert_data: Dict[str, Any],
    customer_data: Dict[str, Any],
    transactions: List[Dict[str, Any]],
    ai_analysis: Dict[str, Any],
    analyst_info: Dict[str, Any],
) -> bytes:
    """Generate a SAR PDF from alert and investigation data."""
    
    if transactions:
        dates = [t.get('date', '') for t in transactions if t.get('date')]
        date_start = min(dates)[:10] if dates else ''
        date_end = max(dates)[:10] if dates else ''
    else:
        date_start = date_end = ''
    
    sar_data = SARData(
        filing_institution="First National Bank",
        sar_reference_number=f"SAR-{alert_data.get('alert_id', 'UNKNOWN')}",
        subject_name=alert_data.get('customer_name', ''),
        subject_type="Business" if any(x in alert_data.get('customer_name', '') for x in ['LLC', 'Inc', 'Corp', 'and']) else "Individual",
        subject_account_number=customer_data.get('account_number', 'On file'),
        subject_occupation=customer_data.get('occupation', ''),
        activity_date_range_start=date_start,
        activity_date_range_end=date_end,
        total_amount=float(alert_data.get('total_amount', 0)),
        transaction_count=len(transactions),
        scenario_type=str(alert_data.get('alert_id', '')),
        scenario_name=alert_data.get('scenario_name', ''),
        alert_score=alert_data.get('alert_score', 0),
        transactions=transactions,
        narrative_summary=ai_analysis.get('summary', ''),
        narrative_activity_details=ai_analysis.get('activity_details', ''),
        narrative_conclusion=ai_analysis.get('conclusion', ''),
        ai_confidence_score=ai_analysis.get('confidence_score', 92),
        ai_recommendation=ai_analysis.get('recommendation', 'SAR Filing Recommended'),
        ai_rationale=ai_analysis.get('rationale', ''),
        analyst_name=analyst_info.get('name', ''),
        analyst_team=analyst_info.get('team', ''),
        supervisor_name=analyst_info.get('supervisor', ''),
    )
    
    return generate_sar_pdf(sar_data)
