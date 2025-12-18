import asyncio
from datetime import datetime

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.schemas import PrepareRequest, DecisionRequest, TransactionStatus
from app.models import DistributedTransaction


class CoordinatorService:

    async def execute_2pc(self, db: AsyncSession, transaction_id: str):
        transaction = await db.get(DistributedTransaction, transaction_id)
        if not transaction:
            return

        participant_urls = transaction.participant_urls
        api_base = "/api"

        votes = {}
        all_yes = True

        # Phase 1: PREPARE

        async with httpx.AsyncClient(
            timeout=settings.prepare_timeout / 1000
        ) as client:

            tasks = []
            for url in participant_urls:
                req = PrepareRequest(
                    transaction_id=transaction_id,
                    operation_type=transaction.operation_type,
                    operation_data=transaction.operation_data,
                )
                tasks.append(
                    client.post(f"{url}{api_base}/prepare", json=req.dict())
                )

            responses = await asyncio.gather(
                *tasks, return_exceptions=True
            )

        for resp, url in zip(responses, participant_urls):
            if isinstance(resp, Exception) or resp.status_code != 200:
                votes[url] = "no"
                all_yes = False
            else:
                vote = resp.json().get("vote", "no")
                votes[url] = vote
                if vote != "yes":
                    all_yes = False

        # Persist votes immediately
        transaction.participant_votes = votes
        transaction.status = (
            TransactionStatus.COMMITTING
            if all_yes
            else TransactionStatus.ABORTING
        )
        await db.commit()

        # Phase 2: DECISION
        
        decision = "commit" if all_yes else "abort"

        async with httpx.AsyncClient(
            timeout=settings.commit_timeout / 1000
        ) as client:

            tasks = []
            for url in participant_urls:
                req = DecisionRequest(
                    transaction_id=transaction_id,
                    decision=decision,
                )
                tasks.append(
                    client.post(
                        f"{url}{api_base}/{decision}",
                        json=req.dict()
                    )
                )

            await asyncio.gather(*tasks, return_exceptions=True)

        transaction.status = (
            TransactionStatus.COMMITTED
            if all_yes
            else TransactionStatus.ABORTED
        )
        transaction.decision_made_at = datetime.utcnow()
        await db.commit()


coordinator_service = CoordinatorService()
