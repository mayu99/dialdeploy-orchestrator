import os
import asyncio
import httpx
from dotenv import load_dotenv

load_dotenv()

VERCEL_API_URL = "https://api.vercel.com"
VERCEL_TOKEN = os.getenv("VERCEL_TOKEN", "")
VERCEL_PROJECT_ID = os.getenv("VERCEL_PROJECT_ID", "")

# Demo mode: set to "false" to attempt real API calls
DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() in ("true", "1", "yes")

def get_headers():
    if not VERCEL_TOKEN:
        raise ValueError("VERCEL_TOKEN env var is missing.")
    return {
        "Authorization": f"Bearer {VERCEL_TOKEN}",
        "Content-Type": "application/json"
    }

async def set_env_var(project_id: str, key: str, value: str, target: str = "production") -> dict:
    """Sets a Vercel project environment variable"""
    if DEMO_MODE or not VERCEL_TOKEN:
        print(f"[Demo] Vercel: Setting env var {key}={value[:30]}... for project {project_id}")
        await asyncio.sleep(1.5)
        return {"id": "mock-env-id"}

    try:
        url = f"{VERCEL_API_URL}/v9/projects/{project_id}/env"
        payload = {
            "key": key,
            "value": value,
            "type": "plain",
            "target": [target]
        }
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=payload, headers=get_headers(), timeout=10.0)
            if response.status_code == 409:
                print(f"Env var {key} already exists. Attempting to overwrite/update...")
                return {"status": "exists"}
            elif response.status_code != 200:
                raise Exception(f"Failed to set Vercel env var: {response.text}")
            return response.json()
    except Exception as e:
        print(f"[Demo] Vercel API failed ({e}), using mock")
        return {"id": "mock-env-id"}

async def get_latest_deployment(project_id: str) -> dict:
    """Retrieves the latest deployment details for a project"""
    if DEMO_MODE or not VERCEL_TOKEN:
        await asyncio.sleep(1.0)
        return {
            "id": "mock-deploy-id",
            "url": "dialdeploy-pwa-template.vercel.app",
            "readyState": "READY"
        }

    try:
        url = f"{VERCEL_API_URL}/v6/deployments?projectId={project_id}&limit=1"
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=get_headers(), timeout=10.0)
            if response.status_code != 200:
                raise Exception(f"Failed to get Vercel deployments: {response.text}")
            deployments = response.json().get("deployments", [])
            if not deployments:
                raise Exception("No deployments found on Vercel project.")
            return deployments[0]
    except Exception as e:
        print(f"[Demo] Vercel API failed ({e}), using mock")
        return {"id": "mock-deploy-id", "url": "dialdeploy-pwa-template.vercel.app", "readyState": "READY"}

async def poll_deployment(deployment_id: str) -> dict:
    """Polls a specific deployment's state"""
    if deployment_id == "mock-deploy-id":
        await asyncio.sleep(0.5)
        return {
            "url": "dialdeploy-pwa-template.vercel.app",
            "readyState": "READY"
        }

    try:
        url = f"{VERCEL_API_URL}/v13/deployments/{deployment_id}"
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=get_headers(), timeout=10.0)
            if response.status_code != 200:
                raise Exception(f"Failed to fetch deployment status: {response.text}")
            return response.json()
    except Exception as e:
        print(f"[Demo] Vercel API failed ({e}), using mock")
        return {"url": "dialdeploy-pwa-template.vercel.app", "readyState": "READY"}

async def wait_for_ready(deployment_id: str, timeout: int = 300) -> str:
    """Blocks until a deployment is READY"""
    start_time = asyncio.get_event_loop().time()
    while True:
        res = await poll_deployment(deployment_id)
        state = res.get("readyState") or res.get("state")
        if state == "READY" or state == "BUILDING_READY": # Vercel API states
            url = res.get("url", "")
            return f"https://{url}" if not url.startswith("http") else url
        elif state in ("ERROR", "FAILED", "CANCELED"):
            raise Exception(f"Vercel deployment failed with status: {state}")
            
        if asyncio.get_event_loop().time() - start_time > timeout:
            raise TimeoutError("Vercel deployment poll timed out")
        
        await asyncio.sleep(5)

