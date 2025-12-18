import asyncio
import httpx

from ..config import settings

class FailureDetector:
    async def start(self):
        if settings.node_role == "coordinator":
            asyncio.create_task(self._monitor_participants())

    async def _monitor_participants(self):
        async with httpx.AsyncClient() as client:
            while True:
                for url in settings.get_participant_urls():
                    try:
                        await client.get(f"{url}/api/health", timeout=5)
                    except:
                        print(f"Participant {url} appears down")
                await asyncio.sleep(settings.heartbeat_interval / 1000)

    async def stop(self):
        pass 