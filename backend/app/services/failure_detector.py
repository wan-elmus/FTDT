import asyncio
import httpx
from datetime import datetime

from ..config import settings

class FailureDetector:
    def __init__(self):
        self.node_health = {}
        self.task = None

    async def start(self):
        if settings.node_role == "coordinator":
            self.task = asyncio.create_task(self._monitor_participants())

    async def _monitor_participants(self):
        async with httpx.AsyncClient(timeout=5.0) as client:
            while True:
                current_time = datetime.utcnow()
                for url in settings.get_participant_urls():
                    node_id = next((k for k, v in settings.nodes.items() if v["url"] == url), "unknown")
                    try:
                        response = await client.get(f"{url}/api/health")
                        if response.status_code == 200:
                            self.node_health[node_id] = {
                                "status": "online",
                                "last_heartbeat": current_time,
                                "uptime": self.node_health.get(node_id, {}).get("uptime", 0) + 1
                            }
                    except Exception:
                        self.node_health[node_id] = {
                            "status": "offline",
                            "last_heartbeat": self.node_health.get(node_id, {}).get("last_heartbeat"),
                            "uptime": self.node_health.get(node_id, {}).get("uptime", 0)
                        }
                        print(f"Participant {url} ({node_id}) appears down")

                await asyncio.sleep(settings.heartbeat_interval / 1000)

    async def stop(self):
        if self.task:
            self.task.cancel()

# Global singleton instance
failure_detector = FailureDetector()