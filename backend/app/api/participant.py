from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime

from ..database import get_db
from ..config import settings
from ..schemas import PrepareRequest, DecisionRequest, VoteResponse
from ..services.participant_service import ParticipantService
from ..models import Account

router = APIRouter()
participant_service = ParticipantService()

@router.post("/prepare", response_model=VoteResponse)
async def prepare(request: PrepareRequest, db: AsyncSession = Depends(get_db)):
    if settings.node_role != "participant":
        raise HTTPException(status_code=403, detail="Only participants accept prepare requests")

    try:
        vote = await participant_service.prepare_transaction(
            db=db,
            transaction_id=request.transaction_id,
            operation_type=request.operation_type,
            operation_data=request.operation_data
        )
        return VoteResponse(
            transaction_id=request.transaction_id,
            vote=vote,
            node_id=settings.node_id,
            message="Prepared successfully" if vote == "yes" else "Cannot prepare"
        )
    except Exception as e:
        return VoteResponse(
            transaction_id=request.transaction_id,
            vote="no",
            node_id=settings.node_id,
            message=str(e)
        )

@router.post("/commit")
async def commit(request: DecisionRequest, db: AsyncSession = Depends(get_db)):
    if settings.node_role != "participant":
        raise HTTPException(status_code=403, detail="Only participants accept commit")

    await participant_service.commit_transaction(db, request.transaction_id)
    return {"status": "committed", "transaction_id": request.transaction_id}

@router.post("/abort")
async def abort(request: DecisionRequest, db: AsyncSession = Depends(get_db)):
    if settings.node_role != "participant":
        raise HTTPException(status_code=403, detail="Only participants accept abort")

    await participant_service.abort_transaction(db, request.transaction_id)
    return {"status": "aborted", "transaction_id": request.transaction_id}

@router.post("/recover")
async def recover(db: AsyncSession = Depends(get_db)):
    if settings.node_role != "participant":
        raise HTTPException(status_code=403, detail="Only participants can recover")

    recovered = await participant_service.recover_uncertain_transactions(db)
    return {"message": "Recovery completed", "recovered_count": len(recovered)}

@router.get("/accounts")
async def list_accounts(db: AsyncSession = Depends(get_db)):
    from sqlalchemy import select

    result = await db.execute(select(Account))
    accounts = result.scalars().all()

    return [
        {
            "id": a.id,
            "balance": a.balance,
            "node_id": a.node_id,
            "created_at": a.created_at.isoformat() if a.created_at else None,
            "updated_at": a.updated_at.isoformat() if a.updated_at else None,
        }
        for a in accounts
    ]