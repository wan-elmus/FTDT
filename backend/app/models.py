import uuid
from datetime import datetime
from enum import Enum
from app.database import Base

from sqlalchemy import (
    Column,
    String,
    Integer,
    DateTime,
    JSON,
    Text,
    Boolean,
    Enum as SQLEnum,
    UniqueConstraint,
    Index,
)
class TransactionStatus(str, Enum):
    INIT = "init"
    PREPARING = "preparing"
    PREPARED = "prepared"
    COMMITTING = "committing"
    COMMITTED = "committed"
    ABORTING = "aborting"
    ABORTED = "aborted"


class LockType(str, Enum):
    READ = "read"
    WRITE = "write"


class Account(Base):
    __tablename__ = "accounts"
    __table_args__ = (
        UniqueConstraint("id", "node_id", name="uq_account_node"),
        Index("idx_account_node", "node_id", "id"),
    )

    id = Column(String(50), primary_key=True, index=True)
    balance = Column(Integer, default=0, nullable=False)
    node_id = Column(String(50), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class DistributedTransaction(Base):
    __tablename__ = "distributed_transactions"
    __table_args__ = (
        Index("idx_transaction_status", "status"),
        Index("idx_transaction_timeout", "timeout_at"),
    )

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    status = Column(SQLEnum(TransactionStatus), default=TransactionStatus.INIT, index=True)

    operation_type = Column(String(50), nullable=False)
    operation_data = Column(JSON, nullable=False)

    participant_urls = Column(JSON, nullable=False)
    participant_votes = Column(JSON, default=dict)
    participant_decisions = Column(JSON, default=dict)

    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    prepare_started_at = Column(DateTime, nullable=True)
    decision_made_at = Column(DateTime, nullable=True)
    timeout_at = Column(DateTime, nullable=True)

    recovery_attempts = Column(Integer, default=0)
    last_recovery_attempt = Column(DateTime, nullable=True)


class LocalTransaction(Base):
    __tablename__ = "local_transactions"
    __table_args__ = (
        UniqueConstraint("transaction_id", "node_id", name="uq_local_transaction"),
        Index("idx_local_transaction_status", "node_id", "status"),
        Index("idx_local_transaction_vote", "transaction_id", "vote"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String(36), nullable=False, index=True)
    node_id = Column(String(50), nullable=False, index=True)

    status = Column(SQLEnum(TransactionStatus), default=TransactionStatus.INIT, index=True)
    vote = Column(String(10), nullable=True)

    operation_type = Column(String(50))
    operation_data = Column(JSON)

    before_state = Column(JSON, nullable=True)
    after_state = Column(JSON, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    prepared_at = Column(DateTime, nullable=True)
    decided_at = Column(DateTime, nullable=True)


class TransactionLog(Base):
    __tablename__ = "transaction_logs"
    __table_args__ = (
        Index("idx_log_transaction", "transaction_id", "node_id"),
        Index("idx_log_applied", "applied", "created_at"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String(36), nullable=False, index=True)
    node_id = Column(String(50), nullable=False, index=True)

    log_type = Column(String(50), nullable=False)
    old_state = Column(JSON, nullable=True)
    new_state = Column(JSON, nullable=True)
    details = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    applied = Column(Boolean, default=False)


class Lock(Base):
    __tablename__ = "locks"
    __table_args__ = (
        UniqueConstraint(
            "resource_type",
            "resource_id",
            "node_id",
            "lock_type",
            name="uq_lock_resource",
        ),
        Index("idx_lock_transaction", "transaction_id", "node_id"),
        Index("idx_lock_resource", "resource_type", "resource_id", "node_id"),
        Index("idx_lock_active", "released_at"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)

    resource_type = Column(String(50), nullable=False)
    resource_id = Column(String(100), nullable=False)
    node_id = Column(String(50), nullable=False, index=True)

    lock_type = Column(SQLEnum(LockType), nullable=False)
    transaction_id = Column(String(36), nullable=False, index=True)

    acquired_at = Column(DateTime, default=datetime.utcnow)
    released_at = Column(DateTime, nullable=True)