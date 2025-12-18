from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from datetime import datetime

from app.models import LocalTransaction, Account, TransactionStatus, TransactionLog
from app.services.lock_manager import lock_manager
from app.services.transaction_manager import transaction_manager
from app.config import settings

class ParticipantService:
    async def prepare_transaction(
        self,
        db: AsyncSession,
        transaction_id: str,
        operation_type: str,
        operation_data: dict
    ) -> str:
        local_tx = LocalTransaction(
            transaction_id=transaction_id,
            node_id=settings.node_id,
            status=TransactionStatus.PREPARING,
            operation_type=operation_type,
            operation_data=operation_data
        )
        db.add(local_tx)
        await db.flush()

        if operation_type == "transfer":
            from_account = operation_data.get("from_account")
            to_account = operation_data.get("to_account")
            amount = operation_data.get("amount")

            local_accounts = []
            if operation_data.get("from_node") == settings.node_id:
                local_accounts.append(("from", from_account))
            if operation_data.get("to_node") == settings.node_id:
                local_accounts.append(("to", to_account))

            for role, acc_id in local_accounts:
                locked = await lock_manager.acquire_write_lock(
                    db=db,
                    transaction_id=transaction_id,
                    resource_id=acc_id
                )
                if not locked:
                    local_tx.status = TransactionStatus.ABORTED
                    local_tx.vote = "no"
                    await db.commit()
                    return "no"

                account = await db.get(Account, acc_id)
                if not account:
                    await lock_manager.release_all_locks(db, transaction_id)
                    local_tx.status = TransactionStatus.ABORTED
                    local_tx.vote = "no"
                    await db.commit()
                    return "no"

                if role == "from" and account.balance < amount:
                    await lock_manager.release_all_locks(db, transaction_id)
                    local_tx.status = TransactionStatus.ABORTED
                    local_tx.vote = "no"
                    await db.commit()
                    return "no"

                before = {"balance": account.balance}
                after = {"balance": account.balance - amount if role == "from" else account.balance + amount}
                await transaction_manager.log_prepare(
                    db=db,
                    transaction_id=transaction_id,
                    before_state=before,
                    after_state=after,
                    details=f"{role} account {acc_id}"
                )

        local_tx.status = TransactionStatus.PREPARED
        local_tx.vote = "yes"
        local_tx.prepared_at = datetime.utcnow()
        await db.commit()
        return "yes"

    async def commit_transaction(self, db: AsyncSession, transaction_id: str):
        query = select(LocalTransaction).where(
            and_(
                LocalTransaction.transaction_id == transaction_id,
                LocalTransaction.node_id == settings.node_id
            )
        )
        result = await db.execute(query)
        local_tx = result.scalar_one_or_none()

        if not local_tx or local_tx.status == TransactionStatus.COMMITTED:
            return  # Idempotent: already committed
        
        if local_tx.operation_type == "transfer":
            op = local_tx.operation_data
            from_account = op.get("from_account")
            to_account = op.get("to_account")
            amount = op.get("amount")
            from_node = op.get("from_node")
            to_node = op.get("to_node")

            if from_node == settings.node_id and from_account:
                account = await db.get(Account, from_account)
                if account:
                    account.balance -= amount
                    account.updated_at = datetime.utcnow()
                    db.add(account)

            if to_node == settings.node_id and to_account:
                account = await db.get(Account, to_account)
                if account:
                    account.balance += amount
                    account.updated_at = datetime.utcnow()
                    db.add(account)

        # Finalize
        local_tx.status = TransactionStatus.COMMITTED
        local_tx.decided_at = datetime.utcnow()
        await transaction_manager.log_commit(db, transaction_id)
        await lock_manager.release_all_locks(db, transaction_id)
        await db.commit()

    async def abort_transaction(self, db: AsyncSession, transaction_id: str):
        query = select(LocalTransaction).where(
            and_(
                LocalTransaction.transaction_id == transaction_id,
                LocalTransaction.node_id == settings.node_id
            )
        )
        result = await db.execute(query)
        local_tx = result.scalar_one_or_none()

        if local_tx and local_tx.status not in {TransactionStatus.COMMITTED, TransactionStatus.ABORTED}:
            local_tx.status = TransactionStatus.ABORTED
            local_tx.decided_at = datetime.utcnow()
            await transaction_manager.log_abort(db, transaction_id)
            await lock_manager.release_all_locks(db, transaction_id)
            await db.commit()

    async def recover_uncertain_transactions(self, db: AsyncSession) -> list:
        # Fixed: Add node_id filter
        query = select(LocalTransaction).where(
            and_(
                LocalTransaction.node_id == settings.node_id,
                LocalTransaction.status == TransactionStatus.PREPARED
            )
        )
        result = await db.execute(query)
        uncertain = result.scalars().all()

        recovered = []
        for tx in uncertain:
            await self.abort_transaction(db, tx.transaction_id)
            recovered.append(tx.transaction_id)

        return recovered

participant_service = ParticipantService()