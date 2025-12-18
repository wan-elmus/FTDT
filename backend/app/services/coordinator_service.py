import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
import httpx 
from datetime import datetime, timedelta

from app.config import settings
from app.schemas import PrepareRequest, DecisionRequest, TransactionStatus

class CoordinatorService:
    async def execute_2pc(self, db: AsyncSession, transaction_id: str):
        # Get transaction
        from ..models import DistributedTransaction
        transaction = await db.get(DistributedTransaction, transaction_id)
        if not transaction:
            return

        # Phase 1: Prepare
        prepare_requests = []
        async with httpx.AsyncClient(timeout=settings.prepare_timeout / 1000) as client:
            for url in transaction.participant_urls:
                prepare_req = PrepareRequest(
                    transaction_id=transaction_id,
                    operation_type=transaction.operation_type,
                    operation_data=transaction.operation_data
                )
                prepare_requests.append(
                    client.post(f"{url}/prepare", json=prepare_req.dict())
                )

            responses = await asyncio.gather(*prepare_requests, return_exceptions=True)

        votes = {}
        all_yes = True
        for resp, url in zip(responses, transaction.participant_urls):
            if isinstance(resp, Exception) or resp.status_code != 200:
                votes[url] = "no"
                all_yes = False
            else:
                vote = resp.json().get("vote")
                votes[url] = vote
                if vote != "yes":
                    all_yes = False

        transaction.participant_votes = votes

        # Phase 2: Decision
        decision = "commit" if all_yes else "abort"
        transaction.status = TransactionStatus.COMMITTING if all_yes else TransactionStatus.ABORTING

        async with httpx.AsyncClient() as client:
            decision_requests = []
            for url in transaction.participant_urls:
                req = DecisionRequest(
                    transaction_id=transaction_id,
                    decision=decision
                    )
                endpoint = "/commit" if decision == "commit" else "/abort"
                decision_requests.append(
                    client.post(f"{url}{endpoint}", json=req.dict())
                )
            await asyncio.gather(*decision_requests, return_exceptions=True)

        transaction.status = TransactionStatus.COMMITTED if all_yes else TransactionStatus.ABORTED
        transaction.decision_made_at = datetime.utcnow()
        await db.commit()

coordinator_service = CoordinatorService()