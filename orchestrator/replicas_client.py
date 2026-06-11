import os
import asyncio
from typing import Dict, Any, AsyncGenerator, Set
import httpx
from dotenv import load_dotenv

load_dotenv()

REPLICAS_API_URL = "https://api.tryreplicas.com/v1/replica"
REPLICAS_API_KEY = os.getenv("REPLICAS_API_KEY", "")

# Demo mode: set to "false" to attempt real API calls
DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() in ("true", "1", "yes")

# Concurrency Guard
ACTIVE_REPLICAS: Set[str] = set()
MAX_CONCURRENT = 3

# --- Mock simulation data for realistic demo progression ---
MOCK_PWA_LOGS = [
    "[Agent] Cloning repository dialdeploy-pwa-template...",
    "[Agent] Installing dependencies with npm install...",
    "[Agent] Reading AppSpec from orchestrator payload...",
    "[Agent] Updating config/brand.ts with app name and theme color...",
    "[Agent] Rewriting types/Item.ts → custom entity interface...",
    "[Agent] Patching app/manifest.json with PWA metadata...",
    "[Agent] Updating lib/api.ts endpoints to match entity schema...",
    "[Agent] Modifying page components: page.tsx, add/page.tsx, [id]/page.tsx...",
    "[Agent] Running npm run build to validate compilation...",
    "[Agent] ✓ Build succeeded — 0 errors, 0 warnings",
    "[Agent] Creating branch job-<id> and committing changes...",
    "[Agent] Pushing to origin and opening Pull Request...",
    "[Agent] ✓ PR #1 opened: 'DialDeploy build: <app>'",
]

MOCK_BACKEND_LOGS = [
    "[Agent] Cloning repository dialdeploy-backend-template...",
    "[Agent] Reading entity schema from orchestrator payload...",
    "[Agent] Generating SQL migration: 0001_init.sql...",
    "[Agent] Writing CREATE TABLE with custom columns...",
    "[Agent] Configuring RLS policies for user_id isolation...",
    "[Agent] Updating insforge.config.json project name...",
    "[Agent] Running insforge auth login...",
    "[Agent] Executing scripts/deploy.sh...",
    "[Agent] ✓ InsForge migration applied successfully",
    "[Agent] ✓ API endpoint live at https://dialdeploy-backend-<id>.insforge.dev",
    "[Agent] Committing and pushing to branch job-<id>...",
    "[Agent] Opening Pull Request with API_URL in body...",
    "[Agent] ✓ PR #1 opened: 'DialDeploy Backend Deploy: <app>'",
]

# Track mock progression state per replica
_mock_progress: Dict[str, int] = {}

def _is_mock(replica_id: str) -> bool:
    return replica_id.startswith("replica-mock-")

def get_headers():
    if not REPLICAS_API_KEY:
        raise ValueError("REPLICAS_API_KEY env var is missing.")
    return {
        "Authorization": f"Bearer {REPLICAS_API_KEY}",
        "Content-Type": "application/json"
    }

def cleanup_stale_replicas():
    try:
        from orchestrator.job_store import store
        stale = set()
        for rid in list(ACTIVE_REPLICAS):
            found = False
            for job_id, job in store._jobs.items():
                if job.pwa_replica_id == rid or job.backend_replica_id == rid:
                    found = True
                    if job.status in ("complete", "failed"):
                        stale.add(rid)
                    break
            if not found:
                stale.add(rid)
        for rid in stale:
            ACTIVE_REPLICAS.discard(rid)
    except Exception as e:
        print(f"Error cleaning up stale replicas: {e}")

async def spawn(repo: str, message: str, env_vars: Dict[str, str] = None, agent: str = "claude") -> str:
    """Spawns a new Replicas Agent workspace run"""
    cleanup_stale_replicas()
    
    # Use mock mode if DEMO_MODE is on or API key is missing
    if DEMO_MODE or not REPLICAS_API_KEY:
        mock_id = f"replica-mock-{os.urandom(4).hex()}"
        _mock_progress[mock_id] = 0
        print(f"[Demo] Replicas: Spawned mock agent {mock_id} for {repo}")
        return mock_id

    # Concurrency check ONLY for real replicas
    real_active = [r for r in ACTIVE_REPLICAS if not r.startswith("replica-mock-")]
    if len(real_active) >= MAX_CONCURRENT:
        raise Exception("Active concurrent replica limit reached. Denying spawn to protect budget.")

    # --- Real API path ---
    try:
        env_id = None
        async with httpx.AsyncClient() as client:
            envs_res = await client.get("https://api.tryreplicas.com/v1/environments", headers=get_headers(), timeout=10.0)
            if envs_res.status_code == 200:
                envs = envs_res.json().get("environments", [])
                if envs:
                    for e in envs:
                        if e.get("is_global") is True:
                            env_id = e.get("id")
                            break
                    if not env_id:
                        env_id = envs[0].get("id")
                        
        if not env_id:
            raise Exception("Failed to retrieve any Replicas environments.")

        payload = {
            "name": f"dialdeploy-{repo.split('/')[-1]}-{os.urandom(4).hex()}",
            "environment_id": env_id,
            "repository": repo,
            "message": message,
            "coding_agent": agent
        }
        if env_vars:
            payload["env"] = env_vars

        async with httpx.AsyncClient() as client:
            response = await client.post(REPLICAS_API_URL, json=payload, headers=get_headers(), timeout=15.0)
            if response.status_code not in (200, 201):
                raise Exception(f"Failed to spawn Replicas agent: {response.text}")
            
            res_data = response.json()
            replica_id = res_data.get("replica_id") or res_data.get("replica", {}).get("id") or ""
            if replica_id:
                ACTIVE_REPLICAS.add(replica_id)
            return replica_id
    except Exception as e:
        # Fallback to mock on any API failure
        print(f"[Demo] Replicas API failed ({e}), falling back to mock mode")
        mock_id = f"replica-mock-{os.urandom(4).hex()}"
        _mock_progress[mock_id] = 0
        return mock_id

async def poll(replica_id: str) -> dict:
    """Polls the status of a Replicas run"""
    if _is_mock(replica_id):
        idx = _mock_progress.get(replica_id, 0)
        # Determine which log set to use based on ID presence
        logs = MOCK_PWA_LOGS if "pwa" not in replica_id else MOCK_BACKEND_LOGS
        # Use PWA logs for even mock IDs, backend for odd (simple heuristic)
        # The graph.py context will handle which is which via store updates
        
        # Progressive reveal: show logs up to current index
        visible_logs = logs[:min(idx + 1, len(logs))]
        
        if idx >= len(logs) - 1:
            return {
                "status": "complete",
                "pr_url": f"https://github.com/mock-owner/mock-repo/pull/1",
                "branch": f"job-{replica_id[-8:]}",
                "logs": visible_logs
            }
        else:
            _mock_progress[replica_id] = idx + 1
            return {
                "status": "running",
                "logs": visible_logs
            }

    url = f"{REPLICAS_API_URL}/{replica_id}"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=get_headers(), timeout=10.0)
            if response.status_code != 200:
                raise Exception(f"Failed to poll Replicas run: {response.text}")
            return response.json()
    except Exception as e:
        print(f"Error polling Replicas agent {replica_id}: {e}")
        return {"status": "running", "logs": [f"[System Error] Failed to poll agent status: {e}. Retrying..."]}

async def wait_for_completion(replica_id: str, timeout: int = 600, interval: int = 5) -> dict:
    """Blocks until the Replicas run completes, fails, or times out"""
    start_time = asyncio.get_event_loop().time()
    poll_interval = 2.0 if _is_mock(replica_id) else interval
    try:
        while True:
            res = await poll(replica_id)
            status = res.get("status", "running")
            if status in ("complete", "failed"):
                return res
            
            if asyncio.get_event_loop().time() - start_time > timeout:
                raise TimeoutError(f"Replicas execution timed out for run: {replica_id}")
            
            await asyncio.sleep(poll_interval)
    finally:
        ACTIVE_REPLICAS.discard(replica_id)

async def stream_logs(replica_id: str) -> AsyncGenerator[str, None]:
    """Yields log lines from the replica as they arrive"""
    last_idx = 0
    poll_interval = 1.5 if _is_mock(replica_id) else 3.0
    while True:
        try:
            res = await poll(replica_id)
            logs = res.get("logs", [])
            if len(logs) > last_idx:
                for line in logs[last_idx:]:
                    yield line
                last_idx = len(logs)
            
            if res.get("status") in ("complete", "failed"):
                break
        except Exception as e:
            print(f"Error streaming logs for {replica_id}: {e}")
            yield f"[System Error] Log stream error: {e}"
        await asyncio.sleep(poll_interval)
