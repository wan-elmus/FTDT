from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import event, text
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from .config import settings


Base = declarative_base()

engine = create_async_engine(
    settings.database_url,
    echo=False,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args={
        "server_settings": {
            "search_path": settings.schema_name
        }
    }
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency for database session.
    
    Important: Does NOT auto-commit. Caller must explicitly commit or rollback.
    This is necessary for 2PC protocol control.
    """
    async with AsyncSessionLocal() as session:
        try:
            # Double-check schema is set
            await session.execute(text(f"SET search_path TO {settings.schema_name}"))
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            # Only close the session, don't commit
            await session.close()


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Context manager for database session with explicit transaction control.
    Use for operations where you need full control over commit/rollback.
    """
    async with AsyncSessionLocal() as session:
        try:
            await session.execute(text(f"SET search_path TO {settings.schema_name}"))
            yield session
        finally:
            await session.close()


async def init_db():
    """Initialize database schema for current node."""
    schema = settings.schema_name
    
    async with engine.begin() as conn:
        if schema != "public":
            await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        
        # Set search path for table creation
        await conn.execute(text(f"SET search_path TO {schema}"))
        
        # Import Base here to avoid circular imports
        from .models import Base
        await conn.run_sync(Base.metadata.create_all)


async def check_db_health() -> bool:
    """Check if database connection is healthy."""
    try:
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False

async def assert_search_path(session: AsyncSession):
    res = await session.execute(text("SHOW search_path"))
    search_path = res.scalar()
    expected = settings.schema_name

    schemas = [s.strip() for s in search_path.split(",")]

    if expected not in schemas or schemas[0] != expected:
        raise RuntimeError(
            f"Invalid search_path: {search_path} (expected {expected})"
        )
