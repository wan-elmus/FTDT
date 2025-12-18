import asyncio
from datetime import datetime
import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.schemas import DecisionRequest, TransactionStatus
from app.models import DistributedTransaction


class CoordinatorService:

    def _derive_participant_operations(self, tx: DistributedTransaction):
        """
        Returns: dict[node_url -> operation_data]
        """
        op = tx.operation_data
        amount = op["amount"]

        operations = {}

        # Debit source node
        from_node = op["from_node"]
        from_url = settings.nodes[from_node]["url"]
        operations[from_url] = {
            "amount": amount,
            "account_id": op["from_account"],
            "delta": -amount,
        }

        # Credit destination node
        to_node = op["to_node"]
        to_url = settings.nodes[to_node]["url"]
        operations[to_url] = {
            "amount": amount,
            "account_id": op["to_account"],
            "delta": amount,
        }

        return operations

    async def execute_2pc(self, db: AsyncSession, transaction_id: str):
        tx = await db.get(DistributedTransaction, transaction_id)
        if not tx:
            return

        participant_ops = self._derive_participant_operations(tx)
        votes = {}
        all_yes = True

        # -------- Phase 1: PREPARE --------
        async with httpx.AsyncClient(
            timeout=settings.prepare_timeout / 1000
        ) as client:

            tasks = []
            urls = []

            for url, op_data in participant_ops.items():
                payload = {
                    "transaction_id": transaction_id,
                    "operation_type": "transfer",
                    "operation_data": {
                        **tx.operation_data,
                        "local_delta": op_data["delta"],
                        "local_account": op_data["account_id"],
                    },
                }
                urls.append(url)
                tasks.append(client.post(f"{url}/api/prepare", json=payload))

            responses = await asyncio.gather(*tasks, return_exceptions=True)

        for resp, url in zip(responses, urls):
            if isinstance(resp, Exception) or resp.status_code != 200:
                votes[url] = "no"
                all_yes = False
            else:
                vote = resp.json().get("vote", "no")
                votes[url] = vote
                if vote != "yes":
                    all_yes = False

        tx.participant_votes = votes
        tx.status = TransactionStatus.COMMITTING if all_yes else TransactionStatus.ABORTING
        await db.commit()

        # -------- Phase 2: COMMIT / ABORT --------
        decision = "commit" if all_yes else "abort"

        async with httpx.AsyncClient(
            timeout=settings.commit_timeout / 1000
        ) as client:

            tasks = []
            for url in participant_ops.keys():
                req = DecisionRequest(
                    transaction_id=transaction_id,
                    decision=decision,
                )
                tasks.append(client.post(f"{url}/api/{decision}", json=req.dict()))

            await asyncio.gather(*tasks, return_exceptions=True)

        tx.status = TransactionStatus.COMMITTED if all_yes else TransactionStatus.ABORTED
        tx.decision_made_at = datetime.utcnow()
        await db.commit()


coordinator_service = CoordinatorService()
