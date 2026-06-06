import os
import time
from typing import Dict, Tuple, Optional
import httpx
from dotenv import load_dotenv

load_dotenv()

DAILY_API_URL = "https://api.daily.co/v1"
DAILY_API_KEY = os.getenv("DAILY_API_KEY", "")

# session_store tracks: room_name -> (user_token, bot_token, job_id)
session_store: Dict[str, Tuple[str, str, str]] = {}

def get_headers():
    if not DAILY_API_KEY:
        raise ValueError("DAILY_API_KEY env var is missing.")
    return {
        "Authorization": f"Bearer {DAILY_API_KEY}",
        "Content-Type": "application/json"
    }

async def create_room() -> dict:
    url = f"{DAILY_API_URL}/rooms"
    # Expiry 1 hour from now
    expiration = int(time.time()) + 3600
    
    payload = {
        "properties": {
            "exp": expiration,
            "enable_chat": False,
            "max_participants": 2
        }
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=payload, headers=get_headers(), timeout=10.0)
        if response.status_code != 200:
            raise Exception(f"Failed to create Daily room: {response.text}")
        return response.json()

async def create_meeting_token(room_name: str, is_bot: bool) -> str:
    url = f"{DAILY_API_URL}/meeting-tokens"
    
    payload = {
        "properties": {
            "room_name": room_name,
            "user_name": "DialDeploy Bot" if is_bot else "Caller",
            "is_owner": is_bot
        }
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=payload, headers=get_headers(), timeout=10.0)
        if response.status_code != 200:
            raise Exception(f"Failed to create meeting token: {response.text}")
        return response.json().get("token", "")

async def delete_room(room_name: str) -> None:
    url = f"{DAILY_API_URL}/rooms/{room_name}"
    async with httpx.AsyncClient() as client:
        response = await client.delete(url, headers=get_headers(), timeout=10.0)
        if response.status_code not in (200, 404):
            raise Exception(f"Failed to delete Daily room: {response.text}")
