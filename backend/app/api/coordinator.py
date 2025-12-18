from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
import uuid

from app.database import get_db, AsyncSessionLocal
from app.config import settings
from app.schemas import (
    TransferRequest,
    TransactionStatusResponse,
    NodeStatus,
)
from app.models import DistributedTransaction, TransactionStatus
from app.services.coordinator_service import coordinator_service
from app.services.failure_detector import failure_detector

router = APIRouter()


@router.post(
    "/transaction/transfer",
    response_model=TransactionStatusResponse
)
async def create_transfer(
    request: TransferRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    if settings.node_role != "coordinator":
        raise HTTPException(
            status_code=403,
            detail="Only coordinator can initiate transactions",
        )

    transaction_id = str(uuid.uuid4())

    transaction = DistributedTransaction(
        id=transaction_id,
        status=TransactionStatus.INIT,
        operation_type="transfer",
        operation_data=request.dict(),
        participant_urls=settings.get_participant_urls(),
        participant_votes={},
        participant_decisions={},
        timeout_at=datetime.utcnow()
        + timedelta(milliseconds=settings.prepare_timeout),
    )

    db.add(transaction)
    await db.commit()
    await db.refresh(transaction)

    # Run 2PC asynchronously (new session inside task)
    background_tasks.add_task(
        _run_2pc_task,
        transaction_id,
    )

    return TransactionStatusResponse(
        transaction_id=transaction.id,
        status=transaction.status,
        votes=transaction.participant_votes,
        decisions=transaction.participant_decisions,
        created_at=transaction.created_at,
        timeout_at=transaction.timeout_at,
    )


async def _run_2pc_task(transaction_id: str):
    async with AsyncSessionLocal() as db:
        await coordinator_service.execute_2pc(
            db=db,
            transaction_id=transaction_id,
        )


@router.get(
    "/transactions/{transaction_id}",
    response_model=TransactionStatusResponse
)
async def get_transaction_status(
    transaction_id: str,
    db: AsyncSession = Depends(get_db),
):
    if settings.node_role != "coordinator":
        raise HTTPException(
            status_code=403,
            detail="Only coordinator can query transaction status",
        )

    transaction = await db.get(
        DistributedTransaction, transaction_id
    )
    if not transaction:
        raise HTTPException(
            status_code=404,
            detail="Transaction not found",
        )

    return TransactionStatusResponse(
        transaction_id=transaction.id,
        status=transaction.status,
        votes=transaction.participant_votes,
        decisions=transaction.participant_decisions,
        created_at=transaction.created_at,
        timeout_at=transaction.timeout_at,
    )


@router.get("/transactions")
async def list_transactions(
    limit: int = 50,
    db: AsyncSession = Depends(get_db),
):
    if settings.node_role != "coordinator":
        raise HTTPException(
            status_code=403,
            detail="Only coordinator can list transactions",
        )

    from sqlalchemy import select, desc

    stmt = (
        select(DistributedTransaction)
        .order_by(desc(DistributedTransaction.created_at))
        .limit(limit)
    )
    result = await db.execute(stmt)
    transactions = result.scalars().all()

    return [
        {
            "transaction_id": tx.id,
            "status": tx.status.value,
            "operation_type": tx.operation_type,
            "created_at": tx.created_at.isoformat(),
            "timeout_at": tx.timeout_at.isoformat()
            if tx.timeout_at
            else None,
            "participants": len(tx.participant_urls),
        }
        for tx in transactions
    ]


@router.get("/nodes")
async def get_nodes_status():
    if settings.node_role != "coordinator":
        raise HTTPException(
            status_code=403,
            detail="Only coordinator can view node status",
        )

    nodes = []

    for node_id, info in settings.nodes.items():
        status = "unknown"
        last_heartbeat = None
        uptime = 0

        if failure_detector and node_id in failure_detector.node_health:
            health = failure_detector.node_health[node_id]
            status = health["status"]
            last_heartbeat = health["last_heartbeat"]
            uptime = health["uptime"]

        nodes.append(
            NodeStatus(
                node_id=node_id,
                role=info["role"],
                url=info["url"],
                status=status,
                last_heartbeat=last_heartbeat,
                uptime=uptime,
            )
        )

    return nodes
