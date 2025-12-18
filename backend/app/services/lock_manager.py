from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from datetime import datetime, timedelta
import asyncio

from app.models import Lock, LockType, TransactionStatus
from app.config import settings

class LockManager:
    """Strict Two-Phase Locking with timeout-based deadlock prevention."""

    async def acquire_write_lock(
        self,
        db: AsyncSession,
        transaction_id: str,
        resource_id: str,  # e.g., account.id
        timeout_ms: int = None
    ) -> bool:
        """Acquire write lock on resource. Returns True if successful."""
        timeout_ms = timeout_ms or settings.lock_timeout

        start_time = datetime.utcnow()
        while (datetime.utcnow() - start_time).total_seconds() * 1000 < timeout_ms:
            # Check if any conflicting lock exists (write or read)
            query = select(Lock).where(
                and_(
                    Lock.resource_id == resource_id,
                    Lock.released_at.is_(None)
                )
            )
            result = await db.execute(query)
            existing_lock = result.scalar_one_or_none()

            if not existing_lock:
                # No conflict — acquire lock
                new_lock = Lock(
                    resource_type="account",
                    resource_id=resource_id,
                    node_id=settings.node_id,
                    lock_type=LockType.WRITE,
                    transaction_id=transaction_id
                )
                db.add(new_lock)
                await db.flush()
                return True

            # Conflict — wait a bit
            await asyncio.sleep(0.1)

        # Timeout
        return False

    async def release_all_locks(self, db: AsyncSession, transaction_id: str):
        """Release all locks held by transaction_id."""
        query = select(Lock).where(
            and_(
                Lock.transaction_id == transaction_id,
                Lock.released_at.is_(None)
            )
        )
        result = await db.execute(query)
        locks = result.scalars().all()

        for lock in locks:
            lock.released_at = datetime.utcnow()

        await db.flush()

lock_manager = LockManager()