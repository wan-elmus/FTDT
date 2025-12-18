from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
import uuid

from app.database import get_db
from app.config import settings
from app.schemas import TransferRequest, TransactionStatusResponse, NodeStatus
from app.models import DistributedTransaction, TransactionStatus
from app.services.coordinator_service import CoordinatorService
from app.services.failure_detector import failure_detector

router = APIRouter()
coordinator_service = CoordinatorService()

@router.post("/transaction/transfer", response_model=TransactionStatusResponse)
async def create_transfer(
    request: TransferRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    if settings.node_role != "coordinator":
        raise HTTPException(status_code=403, detail="Only coordinator can initiate transactions")

    transaction_id = str(uuid.uuid4())

    operation_data = request.dict()

    transaction = DistributedTransaction(
        id=transaction_id,
        status=TransactionStatus.INIT,
        operation_type="transfer",
        operation_data=operation_data,
        participant_urls=settings.get_participant_urls(),
        participant_votes={},
        participant_decisions={},
        timeout_at=datetime.utcnow() + timedelta(milliseconds=settings.prepare_timeout)
    )

    db.add(transaction)
    await db.commit()
    await db.refresh(transaction)

    # Run 2PC in background
    background_tasks.add_task(
        coordinator_service.execute_2pc,
        db=db,
        transaction_id=transaction_id
    )

    return TransactionStatusResponse(
        transaction_id=transaction.id,
        status=transaction.status,
        votes=transaction.participant_votes,
        decisions=transaction.participant_decisions,
        created_at=transaction.created_at,
        timeout_at=transaction.timeout_at
    )

@router.get("/transactions/{transaction_id}", response_model=TransactionStatusResponse)
async def get_transaction_status(transaction_id: str, db: AsyncSession = Depends(get_db)):
    if settings.node_role != "coordinator":
        raise HTTPException(status_code=403, detail="Only coordinator can query transaction status")

    transaction = await db.get(DistributedTransaction, transaction_id)
    if not transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")

    return TransactionStatusResponse(
        transaction_id=transaction.id,
        status=transaction.status,
        votes=transaction.participant_votes,
        decisions=transaction.participant_decisions,
        created_at=transaction.created_at,
        timeout_at=transaction.timeout_at
    )

@router.get("/transactions")
async def list_transactions(limit: int = 50, db: AsyncSession = Depends(get_db)):
    if settings.node_role != "coordinator":
        raise HTTPException(status_code=403, detail="Only coordinator can list transactions")

    from sqlalchemy import select, desc

    query = select(DistributedTransaction).order_by(desc(DistributedTransaction.created_at)).limit(limit)
    result = await db.execute(query)
    transactions = result.scalars().all()

    return [
        {
            "transaction_id": t.id,
            "status": t.status.value,
            "operation_type": t.operation_type,
            "created_at": t.created_at.isoformat(),
            "timeout_at": t.timeout_at.isoformat() if t.timeout_at else None,
            "participants": len(t.participant_urls)
        }
        for t in transactions
    ]

@router.get("/nodes")
async def get_nodes_status():
    if settings.node_role != "coordinator":
        raise HTTPException(status_code=403, detail="Only coordinator can view node status")

    # Use failure_detector for real health
    nodes = []
    for node_id, info in settings.nodes.items():
        status = "online"
        last_heartbeat = None
        uptime = 0

        # Check if failure_detector has marked it down
        if failure_detector and node_id in failure_detector.node_health:
            health = failure_detector.node_health[node_id]
            status = health["status"]
            last_heartbeat = health["last_heartbeat"]
            uptime = health["uptime"]

        nodes.append(NodeStatus(
            node_id=node_id,
            role=info["role"],
            url=info["url"],
            status=status,
            last_heartbeat=last_heartbeat,
            uptime=uptime
        ))

    return nodes