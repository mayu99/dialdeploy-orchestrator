from pydantic import BaseModel, Field, field_validator
from typing import List, Optional
import uuid

class Entity(BaseModel):
    name: str = Field(..., description="Name of the database/UI entity in Singular title case, e.g., 'Habit'")
    fields: List[str] = Field(..., description="List of simple tracking fields for the entity, e.g., ['title', 'streak_count']")

class AppSpec(BaseModel):
    app_name: str = Field(..., description="The brand name of the mobile PWA application")
    description: str = Field(..., description="A one-line description of what the app does")
    entities: List[Entity] = Field(..., description="1-3 entities that the user wants to track")
    features: List[str] = Field(..., description="2-4 key features of the application")
    primary_color_hex: str = Field("#4F46E5", description="Primary accent theme color in Hex format")
    session_id: str = Field(..., description="The Daily.co WebRTC room session identifier")
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique build job identifier")

    @field_validator('entities')
    @classmethod
    def validate_entities_count(cls, v):
        if not (1 <= len(v) <= 3):
            raise ValueError('Application must have between 1 and 3 entities')
        return v

if __name__ == "__main__":
    # Sample verification
    sample = AppSpec(
        app_name="DailyStreak",
        description="A habit tracker to log and view daily streak achievements",
        entities=[
            Entity(name="Habit", fields=["title", "frequency", "streak_count"])
        ],
        features=[
            "Log a habit completion",
            "View active habit list",
            "Track consecutive completion days"
        ],
        primary_color_hex="#4F46E5",
        session_id="room-abc-123"
    )
    print("Sample validation succeeded:")
    print(sample.model_dump_json(indent=2))
