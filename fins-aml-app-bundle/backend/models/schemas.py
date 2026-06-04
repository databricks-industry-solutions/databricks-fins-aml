"""
Pydantic models for API request/response schemas
"""

from datetime import datetime, date
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

# ==============================================================================
# Executive Dashboard Models
# ==============================================================================

class KPIModel(BaseModel):
    """Executive KPI card data"""
    label: str
    value: str
    change: str
    trend: str  # 'up', 'down', 'neutral'

class TeamPerformanceModel(BaseModel):
    """Team performance metrics"""
    team_name: str
    cases_closed: int
    avg_time_hours: float
    sar_rate: float
    quality_score: float

class SankeyDataModel(BaseModel):
    """Sankey diagram data"""
    scenario_name: str
    team: str
    status: str
    case_count: int

# ==============================================================================
# Analyst Dashboard Models
# ==============================================================================

class AnalystModel(BaseModel):
    """Analyst information"""
    analyst_id: Optional[str] = None
    analyst_name: str
    team_name: str
    role: str

class AlertModel(BaseModel):
    """Alert queue item"""
    alert_id: str
    customer_id: str
    customer_name: str
    scenario_name: str
    alert_score: int
    priority: str  # 'critical', 'high', 'medium', 'low'
    total_amount: str
    alert_status: str
    days_open: int

class AlertStatsModel(BaseModel):
    """Alert statistics for analyst dashboard"""
    total_alerts: int
    new_alerts: int
    in_progress_alerts: int
    avg_score: float

# ==============================================================================
# Investigation Models
# ==============================================================================

class AlertDetailModel(BaseModel):
    """Detailed alert information for investigation"""
    alert_id: str
    customer_id: str
    customer_name: str
    customer_type: str
    scenario_name: str
    alert_score: int
    priority: str
    total_amount: str
    risk_rating: str
    kyc_status: str
    pep_flag: bool
    created_date: datetime

class TransactionModel(BaseModel):
    """Transaction data"""
    transaction_id: str
    transaction_date: datetime  # Changed from date to datetime to handle timestamps from DB
    amount: float
    transaction_type: str
    location: Optional[str] = None
    time_of_day: Optional[str] = None
    description: Optional[str] = None

class KeyMetricsModel(BaseModel):
    """Key investigation metrics"""
    total_amount: float
    transaction_count: int
    time_window_days: int
    ctr_breaches: int

class NetworkNodeModel(BaseModel):
    """Network graph node"""
    node_id: str
    node_type: str  # 'customer', 'account', 'transaction'
    name: str
    risk_level: Optional[str] = None

class NetworkEdgeModel(BaseModel):
    """Network graph edge"""
    source: str
    target: str
    relationship_type: str
    weight: Optional[float] = None

class NetworkGraphModel(BaseModel):
    """Complete network graph data"""
    nodes: List[NetworkNodeModel]
    edges: List[NetworkEdgeModel]

# ==============================================================================
# Agent Chat Models
# ==============================================================================

class ChatMessageModel(BaseModel):
    """Chat message"""
    role: str  # 'user', 'assistant'
    content: str
    timestamp: Optional[datetime] = None

class ChatRequestModel(BaseModel):
    """Chat request payload"""
    message: str
    context: Dict[str, Any]
    chat_history: List[ChatMessageModel]

class ChatResponseModel(BaseModel):
    """Chat response from MAS agent"""
    response: str
    confidence_score: Optional[float] = None
    recommendation: Optional[str] = None
    evidence: Optional[List[Dict[str, Any]]] = None

# ==============================================================================
# Case Management Models
# ==============================================================================

class SARFilingModel(BaseModel):
    """SAR filing data"""
    alert_id: str
    narrative: str
    confidence_score: float
    supporting_documents: List[str]
    analyst_notes: str

class CaseDismissalModel(BaseModel):
    """Case dismissal data"""
    alert_id: str
    dismissal_reason: str
    analyst_notes: str

class CaseUpdateModel(BaseModel):
    """Case status update"""
    case_id: str
    status: str
    notes: str

# ==============================================================================
# API Response Models
# ==============================================================================

class ExecutiveDashboardResponse(BaseModel):
    """Executive dashboard API response"""
    kpis: List[KPIModel]
    team_performance: List[TeamPerformanceModel]
    sankey_data: List[SankeyDataModel]

class AnalystDashboardResponse(BaseModel):
    """Analyst dashboard API response"""
    analysts: List[AnalystModel]
    alerts: List[AlertModel]
    stats: AlertStatsModel

class InvestigationResponse(BaseModel):
    """Investigation page API response"""
    alert_detail: AlertDetailModel
    transactions: List[TransactionModel]
    key_metrics: KeyMetricsModel
    network_graph: NetworkGraphModel