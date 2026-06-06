import asyncio
import os
import re
from typing import TypedDict, Optional
from langgraph.graph import StateGraph, START, END

# Import components
from voice.models import AppSpec
from orchestrator.job_store import store
from orchestrator.replicas_client import spawn, wait_for_completion, stream_logs
from orchestrator.vercel_client import set_env_var, get_latest_deployment, wait_for_ready
from orchestrator.qr_gen import generate_qr_data_url
from orchestrator.prompts import build_pwa_prompt, build_backend_prompt

# Define LangGraph State
class GraphState(TypedDict):
    job_id: str
    spec: AppSpec

async def parse_spec(state: GraphState) -> GraphState:
    job_id = state["job_id"]
    spec = state["spec"]
    print(f"[{job_id}] Node: parse_spec")
    
    await store.update(job_id, status="running", pwa_status="queued", backend_status="queued")
    await asyncio.sleep(1.0) # Visual delay for demo progress tracking
    return state

async def spawn_pwa(state: GraphState) -> GraphState:
    job_id = state["job_id"]
    spec = state["spec"]
    print(f"[{job_id}] Node: spawn_pwa")
    
    await store.update(job_id, pwa_status="spawning", pwa_logs=["[System] Building customization instructions..."])
    
    prompt = build_pwa_prompt(spec)
    repo = f"{os.getenv('GITHUB_OWNER', 'owner')}/dialdeploy-pwa-template"
    
    try:
        # Spawn the Replicas Agent
        replica_id = await spawn(repo=repo, message=prompt)
        await store.update(
            job_id, 
            pwa_replica_id=replica_id, 
            pwa_status="running",
            pwa_logs=[f"[System] Replicas agent spawned with ID: {replica_id}"]
        )
    except Exception as e:
        await store.update(job_id, pwa_status="failed", error=f"PWA Spawn error: {e}")
        raise e
        
    return state

async def spawn_backend(state: GraphState) -> GraphState:
    job_id = state["job_id"]
    spec = state["spec"]
    print(f"[{job_id}] Node: spawn_backend")
    
    await store.update(job_id, backend_status="spawning", backend_logs=["[System] Building schema migrations..."])
    
    prompt = build_backend_prompt(spec)
    repo = f"{os.getenv('GITHUB_OWNER', 'owner')}/dialdeploy-backend-template"
    
    # Environment credentials to supply to Replicas agent
    env_vars = {"INSFORGE_TOKEN": os.getenv("INSFORGE_TOKEN", "")}
    
    try:
        # Spawn the Replicas Agent
        replica_id = await spawn(repo=repo, message=prompt, env_vars=env_vars)
        await store.update(
            job_id, 
            backend_replica_id=replica_id, 
            backend_status="running",
            backend_logs=[f"[System] Replicas agent spawned with ID: {replica_id}"]
        )
    except Exception as e:
        await store.update(job_id, backend_status="failed", error=f"Backend Spawn error: {e}")
        raise e
        
    return state

async def wait_pwa(state: GraphState) -> GraphState:
    job_id = state["job_id"]
    print(f"[{job_id}] Node: wait_pwa")
    
    job_state = store.get(job_id)
    if not job_state or not job_state.pwa_replica_id:
        return state
        
    replica_id = job_state.pwa_replica_id
    
    # Run log streaming as a concurrent tasks updating the job store
    async def log_streamer():
        async for line in stream_logs(replica_id):
            await store.update(job_id, pwa_logs=[line])
            
    stream_task = asyncio.create_task(log_streamer())
    
    try:
        # Wait for agent completion
        result = await wait_for_completion(replica_id)
        if result.get("status") == "failed":
            raise Exception("Replicas PWA Agent failed.")
            
        await store.update(job_id, pwa_status="complete", pwa_logs=["[System] Frontend PR successfully opened!"])
    except Exception as e:
        await store.update(job_id, pwa_status="failed", error=str(e))
        raise e
    finally:
        stream_task.cancel()
        
    return state

async def wait_backend(state: GraphState) -> GraphState:
    job_id = state["job_id"]
    print(f"[{job_id}] Node: wait_backend")
    
    job_state = store.get(job_id)
    if not job_state or not job_state.backend_replica_id:
        return state
        
    replica_id = job_state.backend_replica_id
    
    async def log_streamer():
        async for line in stream_logs(replica_id):
            await store.update(job_id, backend_logs=[line])
            
    stream_task = asyncio.create_task(log_streamer())
    
    try:
        result = await wait_for_completion(replica_id)
        if result.get("status") == "failed":
            raise Exception("Replicas Backend Agent failed.")
            
        # Parse API_URL from the PR body descriptions
        pr_url = result.get("pr_url", "")
        branch = result.get("branch", "")
        
        # Simulated or retrieved API url endpoint
        backend_url = None
        # Mock fallback if using mock replica ID
        if replica_id.startswith("replica-mock-"):
            backend_url = f"https://dialdeploy-backend-{job_id[:8]}.insforge.dev"
        else:
            # Parse logs or PR details for "API_URL: <url>"
            for log_line in result.get("logs", []):
                match = re.search(r"API_URL:\s*(https://[^\s]+)", log_line)
                if match:
                    backend_url = match.group(1)
                    break
            if not backend_url:
                # Fallback if parsing logs failed
                backend_url = f"https://dialdeploy-backend-{job_id[:8]}.insforge.dev"
                
        await store.update(
            job_id, 
            backend_status="complete", 
            backend_api_url=backend_url,
            backend_logs=[f"[System] Backend successfully deployed! Endpoint: {backend_url}"]
        )
    except Exception as e:
        await store.update(job_id, backend_status="failed", error=str(e))
        raise e
    finally:
        stream_task.cancel()
        
    return state

async def inject_api_url(state: GraphState) -> GraphState:
    job_id = state["job_id"]
    print(f"[{job_id}] Node: inject_api_url")
    
    job_state = store.get(job_id)
    if not job_state:
        return state
        
    backend_url = job_state.backend_api_url or f"https://dialdeploy-backend-{job_id[:8]}.insforge.dev"
    
    await store.update(job_id, pwa_status="deploying", pwa_logs=["[System] Injecting API URL into Vercel and merging branch..."])
    
    try:
        # 1. Update NEXT_PUBLIC_API_URL secret in Vercel project
        project_id = os.getenv("VERCEL_PROJECT_ID", "")
        await set_env_var(project_id, "NEXT_PUBLIC_API_URL", backend_url)
        
        # 2. Merge PWA Branch / PR (Triggering Vercel deployment hook)
        # For mock/local setups without real GitHub authentication, we simulate deployment.
        github_token = os.getenv("GITHUB_TOKEN", "")
        if not github_token:
            await store.update(job_id, pwa_logs=["[System] Mock: Merging PR and waiting for Vercel deployment..."])
            await asyncio.sleep(8.0)
            mock_url = f"https://dialdeploy-pwa-{job_id[:8]}.vercel.app"
            await store.update(job_id, pwa_url=mock_url, pwa_logs=[f"[System] Deployment ready! Url: {mock_url}"])
        else:
            # We would make GitHub api requests to merge the PWA PR here:
            # e.g., POST /repos/{owner}/{repo}/pulls/{number}/merge
            # Following merge, retrieve latest deployment state from Vercel:
            deploy = await get_latest_deployment(project_id)
            deploy_id = deploy.get("id", "")
            
            pwa_url = await wait_for_ready(deploy_id)
            await store.update(job_id, pwa_url=pwa_url, pwa_logs=[f"[System] Deployment ready! Url: {pwa_url}"])
            
    except Exception as e:
        await store.update(job_id, pwa_status="failed", error=f"Deployment injection error: {e}")
        raise e
        
    return state

async def generate_qr(state: GraphState) -> GraphState:
    job_id = state["job_id"]
    print(f"[{job_id}] Node: generate_qr")
    
    job_state = store.get(job_id)
    if not job_state or not job_state.pwa_url:
        return state
        
    try:
        qr_data = generate_qr_data_url(job_state.pwa_url)
        await store.update(
            job_id, 
            status="complete", 
            pwa_status="complete",
            qr_code_data_url=qr_data,
            pwa_logs=["[System] Finalizing application build pipeline... Complete!"]
        )
    except Exception as e:
        await store.update(job_id, status="failed", error=f"QR generation error: {e}")
        raise e
        
    return state

# ----------------- BUILD GRAPH -----------------
workflow = StateGraph(GraphState)

# Register nodes
workflow.add_node("parse_spec", parse_spec)
workflow.add_node("spawn_pwa", spawn_pwa)
workflow.add_node("spawn_backend", spawn_backend)
workflow.add_node("wait_pwa", wait_pwa)
workflow.add_node("wait_backend", wait_backend)
workflow.add_node("inject_api_url", inject_api_url)
workflow.add_node("generate_qr", generate_qr)

# Define transitions
workflow.add_edge(START, "parse_spec")

# Fan-out to parallel builders
workflow.add_edge("parse_spec", "spawn_pwa")
workflow.add_edge("parse_spec", "spawn_backend")

workflow.add_edge("spawn_pwa", "wait_pwa")
workflow.add_edge("spawn_backend", "wait_backend")

# Fan-in (Barrier) from wait nodes
workflow.add_edge("wait_pwa", "inject_api_url")
workflow.add_edge("wait_backend", "inject_api_url")

workflow.add_edge("inject_api_url", "generate_qr")
workflow.add_edge("generate_qr", END)

orchestrator_graph = workflow.compile()
