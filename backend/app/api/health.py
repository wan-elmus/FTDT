from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime

from ..database import get_db
from ..config import settings
from ..schemas import HealthResponse

router = APIRouter()

@router.get("/health", response_model=HealthResponse)
async def health_check(db: AsyncSession = Depends(get_db)):
    try:
        await db.execute("SELECT 1")
        db_healthy = True
    except Exception:
        db_healthy = False

    return HealthResponse(
        status="healthy" if db_healthy else "unhealthy",
        node_id=settings.node_id,
        timestamp=datetime.utcnow(),
        database=db_healthy
    )