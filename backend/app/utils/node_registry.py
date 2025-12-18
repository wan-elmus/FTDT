import json
from pathlib import Path
from typing import Dict, List, Optional

from ..config import settings

class NodeRegistry:
    """
    Loads and manages node information from nodes.json.
    Provides easy access to URLs, roles, etc.
    """

    def __init__(self):
        self.nodes: Dict[str, Dict] = {}
        self._load()

    def _load(self):
        config_path = Path(__file__).parent.parent / "nodes.json"
        if config_path.exists():
            with open(config_path, 'r') as f:
                self.nodes = json.load(f)
        else:
            raise FileNotFoundError("nodes.json not found in backend root")

    def get_all_nodes(self) -> Dict[str, Dict]:
        return self.nodes

    def get_participant_urls(self) -> List[str]:
        return [
            info["url"]
            for node_id, info in self.nodes.items()
            if info.get("role") == "participant"
        ]

    def get_coordinator_url(self) -> Optional[str]:
        for info in self.nodes.values():
            if info.get("role") == "coordinator":
                return info["url"]
        return None

    def get_node_url(self, node_id: str) -> Optional[str]:
        return self.nodes.get(node_id, {}).get("url")

    def is_participant(self, node_id: str) -> bool:
        return self.nodes.get(node_id, {}).get("role") == "participant"

    def is_coordinator(self, node_id: str) -> bool:
        return self.nodes.get(node_id, {}).get("role") == "coordinator"

# Singleton instance
node_registry = NodeRegistry()