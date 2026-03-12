"""MAL provider configuration."""

from pydantic import BaseModel, Field


class MalListProviderConfig(BaseModel):
    """Configuration for the MAL list provider."""

    token: str = Field(default=..., description="MAL API token for authentication.")
    client_id: str = Field(
        default="b11a4e1ead0db8142268906b4bb676a4",
        description="MAL API client ID for authentication.",
    )
