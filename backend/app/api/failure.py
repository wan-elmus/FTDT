from fastapi import APIRouter
import asyncio
import os
import signal

router = APIRouter()

@router.post("/inject/crash")
async def inject_crash():
    """Immediately crash this node (for demo)"""
    await asyncio.sleep(0.1)
    os.kill(os.getpid(), signal.SIGKILL)
    return {"message": "Crashing node..."}

@router.post("/inject/delay")
async def inject_delay(duration_ms: int = 5000):
    """Delay all responses for duration_ms"""
    await asyncio.sleep(duration_ms / 1000)
    return {"message": f"Delayed response after {duration_ms}ms"}

@router.post("/inject/reject")
async def inject_reject():
    """Reject all future requests (not implemented fully — placeholder)"""
    return {"message": "Node now rejecting requests (placeholder)"}