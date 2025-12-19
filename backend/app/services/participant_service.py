from datetime import datetime

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models import (
    Account,
    LocalTransaction,
    TransactionStatus,
)
from app.services.lock_manager import lock_manager
from app.services.transaction_manager import transaction_manager


class ParticipantService:

    async def prepare_transaction(
        self,
        db: AsyncSession,
        transaction_id: str,
        operation_type: str,
        operation_data: dict,
    ) -> str:
        """
        PREPARE phase:
        - Acquire locks
        - Validate constraints
        - Write prepare log
        - Do NOT modify balances
        """

        local_tx = LocalTransaction(
            transaction_id=transaction_id,
            node_id=settings.node_id,
            status=TransactionStatus.PREPARING,
            operation_type=operation_type,
            operation_data=operation_data,
            created_at=datetime.utcnow(),
        )
        db.add(local_tx)
        await db.flush()

        if operation_type != "transfer":
            local_tx.status = TransactionStatus.PREPARED
            local_tx.vote = "yes"
            local_tx.prepared_at = datetime.utcnow()
            await db.commit()
            return "yes"

        # Participant-scoped operation
        account_id = operation_data["local_account"]
        delta = operation_data["local_delta"]
        amount = abs(delta)

        # Acquire write lock
        locked = await lock_manager.acquire_write_lock(
            db=db,
            transaction_id=transaction_id,
            resource_id=account_id,
        )
        if not locked:
            await self._abort_prepare(db, local_tx, transaction_id)
            return "no"

        # Load account with FOR UPDATE
        stmt = (
            select(Account)
            .where( 
                Account.id == account_id,
                Account.node_id == settings.node_id,
            )
            .with_for_update()
        )
        result = await db.execute(stmt)
        account = result.scalar_one_or_none()

        if not account:
            await self._abort_prepare(db, local_tx, transaction_id)
            return "no"

        # Validation (ONLY for debit)
        if delta < 0 and account.balance < amount:
            await self._abort_prepare(db, local_tx, transaction_id)
            return "no"

        # Write-Ahead Logging (prepare)
        before = {"balance": account.balance}
        after = {"balance": account.balance + delta}

        await transaction_manager.log_prepare(
            db=db,
            transaction_id=transaction_id,
            before_state=before,
            after_state=after,
            details=f"account:{account_id}",
        )

        local_tx.status = TransactionStatus.PREPARED
        local_tx.vote = "yes"
        local_tx.prepared_at = datetime.utcnow()
        await db.commit()
        return "yes"

    async def commit_transaction(self, db: AsyncSession, transaction_id: str):
        """
        COMMIT phase:
        - Apply balance change
        - Write commit log
        - Release locks
        """

        stmt = select(LocalTransaction).where(
            and_(
                LocalTransaction.transaction_id == transaction_id,
                LocalTransaction.node_id == settings.node_id,
            )
        )
        result = await db.execute(stmt)
        local_tx = result.scalar_one_or_none()

        if not local_tx or local_tx.status != TransactionStatus.PREPARED:
            return

        if local_tx.operation_type == "transfer":
            op = local_tx.operation_data
            account_id = op["local_account"]
            delta = op["local_delta"]

            await self._apply_delta(
                db=db,
                account_id=account_id,
                delta=delta,
            )

        local_tx.status = TransactionStatus.COMMITTED
        local_tx.decided_at = datetime.utcnow()

        await transaction_manager.log_commit(db, transaction_id)
        await lock_manager.release_all_locks(db, transaction_id)
        await db.commit()

    async def abort_transaction(self, db: AsyncSession, transaction_id: str):
        """
        ABORT phase:
        - No balance changes
        - Release locks
        - Write abort log
        """

        stmt = select(LocalTransaction).where(
            and_(
                LocalTransaction.transaction_id == transaction_id,
                LocalTransaction.node_id == settings.node_id,
            )
        )
        result = await db.execute(stmt)
        local_tx = result.scalar_one_or_none()

        if not local_tx or local_tx.status in {
            TransactionStatus.COMMITTED,
            TransactionStatus.ABORTED,
        }:
            return

        local_tx.status = TransactionStatus.ABORTED
        local_tx.decided_at = datetime.utcnow()

        await transaction_manager.log_abort(db, transaction_id)
        await lock_manager.release_all_locks(db, transaction_id)
        await db.commit()

    async def recover_uncertain_transactions(self, db: AsyncSession) -> list:
        """
        Recovery:
        Abort all PREPARED transactions (conservative strategy)
        """

        stmt = select(LocalTransaction).where(
            and_(
                LocalTransaction.node_id == settings.node_id,
                LocalTransaction.status == TransactionStatus.PREPARED,
            )
        )
        result = await db.execute(stmt)
        transactions = result.scalars().all()

        recovered = []
        for tx in transactions:
            await self.abort_transaction(db, tx.transaction_id)
            recovered.append(tx.transaction_id)

        return recovered

    async def _apply_delta(
        self,
        db: AsyncSession,
        account_id: str,
        delta: int,
    ):
        stmt = (
            select(Account)
            .where(
                Account.id == account_id,
                Account.node_id == settings.node_id,
            )   
            .with_for_update()
        )
        result = await db.execute(stmt)
        account = result.scalar_one()

        account.balance += delta
        account.updated_at = datetime.utcnow()
        db.add(account)
        await db.flush()

    async def _abort_prepare(
        self,
        db: AsyncSession,
        local_tx: LocalTransaction,
        transaction_id: str,
    ):
        local_tx.status = TransactionStatus.ABORTED
        local_tx.vote = "no"
        await lock_manager.release_all_locks(db, transaction_id)
        await db.commit()


participant_service = ParticipantService()
