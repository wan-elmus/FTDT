from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from datetime import datetime

from app.models import LocalTransaction, TransactionStatus, TransactionLog
from app.config import settings
from app.services.participant_service import participant_service
from app.services.lock_manager import lock_manager

class RecoveryManager:
    """
    Handles crash recovery for participant nodes.
    On startup, resolves any transactions left in PREPARED state.
    """

    async def recover(self, db: AsyncSession):
        if settings.node_role != "participant":
            return []

        # Find uncertain transactions (PREPARED but no decision)
        query = select(LocalTransaction).where(
            and_(
                LocalTransaction.node_id == settings.node_id,
                LocalTransaction.status == TransactionStatus.PREPARED
            )
        )
        result = await db.execute(query)
        uncertain_txs = result.scalars().all()

        recovered = []

        for tx in uncertain_txs:
            # Conservative recovery: abort all uncertain transactions
            # In advanced systems, could contact coordinator, but for this scope: abort
            await participant_service.abort_transaction(db, tx.transaction_id)

            # Clean up locks
            await lock_manager.release_all_locks(db, tx.transaction_id)

            recovered.append({
                "transaction_id": tx.transaction_id,
                "action": "aborted_due_to_recovery"
            })

            # Log recovery action
            recovery_log = TransactionLog(
                transaction_id=tx.transaction_id,
                node_id=settings.node_id,
                log_type="recovery_abort",
                details="Aborted during crash recovery - uncertain state",
                created_at=datetime.utcnow(),
                applied=True
            )
            db.add(recovery_log)

        await db.commit()
        return recovered

recovery_manager = RecoveryManager()