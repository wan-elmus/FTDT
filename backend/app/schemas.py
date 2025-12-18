from typing import Dict, List, Optional, Any
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field

class TransactionStatus(str, Enum):
    INIT = "init"
    PREPARING = "preparing"
    PREPARED = "prepared"
    COMMITTING = "committing"
    COMMITTED = "committed"
    ABORTING = "aborting"
    ABORTED = "aborted"


class TransferRequest(BaseModel):
    from_account: str = Field(..., description="Source account ID")
    to_account: str = Field(..., description="Destination account ID")
    amount: int = Field(..., gt=0, description="Amount to transfer")
    from_node: str = Field(..., description="Node ID of source account")
    to_node: str = Field(..., description="Node ID of destination account")


class PrepareRequest(BaseModel):
    transaction_id: str = Field(..., description="Global transaction ID")
    operation_type: str = Field(..., description="Type of operation")
    operation_data: Dict[str, Any] = Field(..., description="Operation details")


class VoteResponse(BaseModel):
    transaction_id: str
    vote: str = Field(..., pattern="^(yes|no)$")
    node_id: str
    message: Optional[str] = None


class DecisionRequest(BaseModel):
    transaction_id: str
    decision: str = Field(..., pattern="^(commit|abort)$")


class TransactionStatusResponse(BaseModel):
    transaction_id: str
    status: TransactionStatus
    votes: Dict[str, Optional[str]] = {}
    decisions: Dict[str, Optional[str]] = {}
    created_at: datetime
    timeout_at: Optional[datetime] = None


class NodeStatus(BaseModel):
    node_id: str
    role: str
    url: str
    status: str = Field(..., pattern="^(online|offline|recovering)$")
    last_heartbeat: Optional[datetime] = None
    uptime: Optional[int] = None


class AccountInfo(BaseModel):
    id: str
    balance: int
    node_id: str
    created_at: datetime
    updated_at: datetime


class LockInfo(BaseModel):
    resource_type: str
    resource_id: str
    lock_type: str
    transaction_id: str
    acquired_at: datetime
    node_id: str


class FailureInjectionRequest(BaseModel):
    node_id: str
    failure_type: str = Field(..., pattern="^(crash|delay|reject)$")
    duration: Optional[int] = Field(None, gt=0, description="Duration in milliseconds")


class RecoveryRequest(BaseModel):
    node_id: str
    transaction_id: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    node_id: str
    timestamp: datetime
    database: bool
    message: Optional[str] = None


class TransactionOperation(BaseModel):
    type: str
    data: Dict[str, Any]
    node_id: str


class TwoPCState(BaseModel):
    transaction_id: str
    status: TransactionStatus
    participants: List[str]
    votes: Dict[str, Optional[str]] = {}
    decisions: Dict[str, Optional[str]] = {}
    timeout_at: Optional[datetime] = None
    
class ParticipantOperation(BaseModel):
    transaction_id: str
    operation_type: str
    operation_data: Dict[str, Any]