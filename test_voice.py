import pytest
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient
from voice.main import app

client = TestClient(app)

@patch("voice.main.create_room")
@patch("voice.main.create_meeting_token")
@patch("voice.main.run_pipecat_bot")
def test_create_session(mock_run_bot, mock_token, mock_room):
    # Setup mocks
    mock_room.return_value = {"name": "test-room", "url": "https://test.daily.co/test-room"}
    mock_token.side_effect = ["user-token-123", "bot-token-456"]
    
    # Trigger POST request
    response = client.post("/voice/create-session")
    
    # Assertions
    assert response.status_code == 200
    json_data = response.json()
    assert "room_url" in json_data
    assert json_data["room_url"] == "https://test.daily.co/test-room"
    assert "user_token" in json_data
    assert json_data["user_token"] == "user-token-123"
    assert "job_id" in json_data
    assert "session_id" in json_data
    assert json_data["session_id"] == "test-room"
    
    # Verify bot was spawned in background
    mock_run_bot.assert_called_once()

def test_session_status_not_found():
    response = client.get("/voice/sessions/non-existent-room/status")
    assert response.status_code == 200
    assert response.json() == {"active": False}
