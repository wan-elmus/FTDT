from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import logging
from contextlib import asynccontextmanager

from app.config import settings
from app.database import init_db, AsyncSessionLocal
from app.api.coordinator import router as coordinator_router
from app.api.participant import router as participant_router
from app.api.health import router as health_router
from app.api.failure import router as failure_router
from app.services.failure_detector import FailureDetector
from app.services.recovery_manager import recovery_manager

logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(settings.log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

failure_detector = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting {settings.node_role} node: {settings.node_id}")
    logger.info(f"Port: {settings.port} | Schema: {settings.schema_name}")

    await init_db()

    from app.database import assert_search_path

    async with AsyncSessionLocal() as db:
        await assert_search_path(db)
        logger.info(f"Verified search_path = {settings.schema_name}")

    if settings.node_role == "participant":
        async with AsyncSessionLocal() as db:
            recovered = await recovery_manager.recover(db)
            if recovered:
                logger.info(f"Recovery completed: aborted {len(recovered)} uncertain transactions")
            else:
                logger.info("No uncertain transactions found during recovery")

    global failure_detector
    failure_detector = FailureDetector()
    await failure_detector.start()

    yield

    logger.info(f"Shutting down {settings.node_role} node: {settings.node_id}")
    if failure_detector:
        await failure_detector.stop()


app = FastAPI(
    title="Fault-Tolerant Distributed Transactions System",
    description="ICS 2403 Distributed Computing and Applications",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router, prefix="/api", tags=["health"])
app.include_router(failure_router, prefix="/api/failure", tags=["failure"])

if settings.node_role == "coordinator":
    app.include_router(coordinator_router, prefix="/api", tags=["coordinator"])
    logger.info("Running as Coordinator")
elif settings.node_role == "participant":
    app.include_router(participant_router)
    logger.info(f"Running as Participant: {settings.node_id}")
else:
    raise ValueError(f"Invalid NODE_ROLE: {settings.node_role}")

@app.get("/")
async def root():
    return {
        "node_id": settings.node_id,
        "node_role": settings.node_role,
        "port": settings.port,
        "schema": settings.schema_name,
        "coordinator_url": settings.coordinator_url,
        "status": "running"
    }

@app.get("/info")
async def node_info():
    return {
        "node_id": settings.node_id,
        "node_role": settings.node_role,
        "port": settings.port,
        "schema": settings.schema_name,
        "participants": settings.get_participant_urls(),
        "settings": {
            "prepare_timeout": settings.prepare_timeout,
            "commit_timeout": settings.commit_timeout,
            "heartbeat_interval": settings.heartbeat_interval,
            "max_concurrent_transactions": settings.max_concurrent_transactions,
        }
    }

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.port,
        reload=True,
        log_level="info"
    )