import os
import asyncio
from typing import Dict, Any, AsyncGenerator, Set
import httpx
from dotenv import load_dotenv

load_dotenv()

REPLICAS_API_URL = "https://api.tryreplicas.com/v1/replica"
REPLICAS_API_KEY = os.getenv("REPLICAS_API_KEY", "")

# Concurrency Guard
ACTIVE_REPLICAS: Set[str] = set()
MAX_CONCURRENT = 3

def get_headers():
    if not REPLICAS_API_KEY:
        raise ValueError("REPLICAS_API_KEY env var is missing.")
    return {
        "Authorization": f"Bearer {REPLICAS_API_KEY}",
        "Content-Type": "application/json"
    }

async def spawn(repo: str, message: str, env_vars: Dict[str, str] = None, agent: str = "claude") -> str:
    """Spawns a new Replicas Agent workspace run"""
    if len(ACTIVE_REPLICAS) >= MAX_CONCURRENT:
        raise Exception("Active concurrent replica limit reached. Denying spawn to protect budget.")
    
    # In mock mode if API key is empty
    if not REPLICAS_API_KEY:
        mock_id = f"replica-mock-{os.urandom(4).hex()}"
        ACTIVE_REPLICAS.add(mock_id)
        print(f"Mock Replicas: Spawned replica run {mock_id} for repository {repo}")
        return mock_id

    payload = {
        "repository": repo,
        "message": message,
        "coding_agent": agent
    }
    if env_vars:
        payload["env"] = env_vars

    async with httpx.AsyncClient() as client:
        response = await client.post(REPLICAS_API_URL, json=payload, headers=get_headers(), timeout=15.0)
        if response.status_code != 200:
            raise Exception(f"Failed to spawn Replicas agent: {response.text}")
        
        replica_id = response.json().get("replica_id", "")
        if replica_id:
            ACTIVE_REPLICAS.add(replica_id)
        return replica_id

async def poll(replica_id: str) -> dict:
    """Polls the status of a Replicas run"""
    if replica_id.startswith("replica-mock-"):
        # Simulated run progression
        await asyncio.sleep(0.5)
        return {
            "status": "complete",
            "pr_url": "https://github.com/mock-owner/mock-repo/pull/1",
            "branch": f"job-{replica_id[-8:]}",
            "logs": ["Starting agent...", "Reading spec...", "Writing modifications...", "Pushing commits...", "Opening PR..."]
        }

    url = f"{REPLICAS_API_URL}/{replica_id}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=get_headers(), timeout=10.0)
        if response.status_code != 200:
            raise Exception(f"Failed to poll Replicas run: {response.text}")
        return response.json()

async def wait_for_completion(replica_id: str, timeout: int = 600, interval: int = 5) -> dict:
    """Blocks until the Replicas run completes, fails, or times out"""
    start_time = asyncio.get_event_loop().time()
    try:
        while True:
            res = await poll(replica_id)
            status = res.get("status", "running")
            if status in ("complete", "failed"):
                return res
            
            if asyncio.get_event_loop().time() - start_time > timeout:
                raise TimeoutError(f"Replicas execution timed out for run: {replica_id}")
            
            await asyncio.sleep(interval)
    finally:
        ACTIVE_REPLICAS.discard(replica_id)

async def stream_logs(replica_id: str) -> AsyncGenerator[str, None]:
    """Yields log lines from the replica as they arrive"""
    last_idx = 0
    while True:
        res = await poll(replica_id)
        logs = res.get("logs", [])
        if len(logs) > last_idx:
            for line in logs[last_idx:]:
                yield line
            last_idx = len(logs)
        
        if res.get("status") in ("complete", "failed"):
            break
        await asyncio.sleep(3.0)
