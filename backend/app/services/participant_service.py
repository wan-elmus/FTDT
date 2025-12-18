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

        amount = operation_data["amount"]
        from_node = operation_data["from_node"]
        to_node = operation_data["to_node"]

        targets = []

        if from_node == settings.node_id:
            targets.append(("from", operation_data["from_account"]))

        if to_node == settings.node_id:
            targets.append(("to", operation_data["to_account"]))

        for role, acc_id in targets:
            locked = await lock_manager.acquire_write_lock(
                db=db,
                transaction_id=transaction_id,
                resource_id=acc_id,
            )
            if not locked:
                await self._abort_prepare(db, local_tx, transaction_id)
                return "no"

            stmt = (
                select(Account)
                .where(Account.id == acc_id)
                .with_for_update()
            )
            result = await db.execute(stmt)
            account = result.scalar_one_or_none()

            if not account:
                await self._abort_prepare(db, local_tx, transaction_id)
                return "no"

            if role == "from" and account.balance < amount:
                await self._abort_prepare(db, local_tx, transaction_id)
                return "no"

            before = {"balance": account.balance}
            after = {
                "balance": account.balance - amount
                if role == "from"
                else account.balance + amount
            }

            await transaction_manager.log_prepare(
                db=db,
                transaction_id=transaction_id,
                before_state=before,
                after_state=after,
                details=f"{role}:{acc_id}",
            )

        local_tx.status = TransactionStatus.PREPARED
        local_tx.vote = "yes"
        local_tx.prepared_at = datetime.utcnow()
        await db.commit()
        return "yes"

    async def commit_transaction(self, db: AsyncSession, transaction_id: str):
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
            amount = op["amount"]

            if op["from_node"] == settings.node_id:
                await self._apply_delta(
                    db,
                    op["from_account"],
                    -amount,
                )

            if op["to_node"] == settings.node_id:
                await self._apply_delta(
                    db,
                    op["to_account"],
                    amount,
                )

        local_tx.status = TransactionStatus.COMMITTED
        local_tx.decided_at = datetime.utcnow()

        await transaction_manager.log_commit(db, transaction_id)
        await lock_manager.release_all_locks(db, transaction_id)
        await db.commit()

    async def abort_transaction(self, db: AsyncSession, transaction_id: str):
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
            .where(Account.id == account_id)
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
