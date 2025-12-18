from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime

from ..models import TransactionLog
from ..config import settings

class TransactionManager:
    """Helper for write-ahead logging."""

    async def log_prepare(
        self,
        db: AsyncSession,
        transaction_id: str,
        before_state: dict,
        after_state: dict,
        details: str = None
    ):
        log = TransactionLog(
            transaction_id=transaction_id,
            node_id=settings.node_id,
            log_type="prepare",
            old_state=before_state,
            new_state=after_state,
            details=details or "Prepared tentative update",
            applied=False
        )
        db.add(log)

    async def log_commit(self, db: AsyncSession, transaction_id: str):
        log = TransactionLog(
            transaction_id=transaction_id,
            node_id=settings.node_id,
            log_type="commit",
            details="Final commit applied",
            applied=True
        )
        db.add(log)

    async def log_abort(self, db: AsyncSession, transaction_id: str):
        log = TransactionLog(
            transaction_id=transaction_id,
            node_id=settings.node_id,
            log_type="abort",
            details="Transaction aborted - rollback applied",
            applied=True
        )
        db.add(log)

transaction_manager = TransactionManager()