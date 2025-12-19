import os
from typing import List, Optional
from pydantic_settings import BaseSettings
from pathlib import Path
from app.utils.node_registry import node_registry

class Settings(BaseSettings):
    node_id: str = ""
    node_role: str = ""
    port: int = 8086
    coordinator_url: str = ""

    database_url: str = ""

    prepare_timeout: int = 5000
    commit_timeout: int = 3000
    heartbeat_interval: int = 2000
    heartbeat_timeout: int = 5000

    max_concurrent_transactions: int = 10
    lock_timeout: int = 3000

    log_level: str = "INFO"
    log_file: str = "logs/ftdt.log"

    class Config:
        env_file = ".env"
        case_sensitive = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._validate_settings()

    def _validate_settings(self):
        if not self.database_url:
            raise ValueError("DATABASE_URL must be set in .env file")
        if not self.node_id or not self.node_role or not self.port:
            raise ValueError("NODE_ID, NODE_ROLE, and PORT must be set")

    @property
    def schema_name(self) -> str:
        return 'public' if self.node_role == 'coordinator' else self.node_id

    def get_participant_urls(self) -> List[str]:
        return node_registry.get_participant_urls()

    def get_node_url(self, node_id: str) -> Optional[str]:
        return node_registry.get_node_url(node_id)

    def get_coordinator_url(self) -> str:
        return node_registry.get_coordinator_url() or self.coordinator_url

    @property
    def nodes(self):
        return node_registry.get_all_nodes()

settings = Settings()