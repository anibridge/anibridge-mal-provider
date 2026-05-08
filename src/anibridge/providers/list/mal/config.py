"""MAL provider configuration."""

from typing import Annotated

import msgspec


class MalListProviderConfig(msgspec.Struct, kw_only=True):
    """Configuration for the MAL list provider."""

    token: Annotated[
        str,
        msgspec.Meta(description="MAL API token for authentication."),
    ]
    client_id: Annotated[
        str,
        msgspec.Meta(description="MAL API client ID for authentication."),
    ] = "b11a4e1ead0db8142268906b4bb676a4"
    rate_limit: (
        Annotated[
            int,
            msgspec.Meta(
                ge=1,
                description=(
                    "Maximum number of API requests per minute. "
                    "Use null to rely on the shared global default limit."
                ),
            ),
        ]
        | None
    ) = None
