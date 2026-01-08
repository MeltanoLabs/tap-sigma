"""Authentication handler for Sigma Computing API."""

import sys
from typing import Any

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta


class SigmaAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator for Sigma Computing API using OAuth 2.0 client credentials."""

    @override
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        auth_endpoint: str,
        oauth_scopes: str | None = None,
    ) -> None:
        """Initialize authenticator.

        Args:
            client_id: The client ID for the Sigma Computing API.
            client_secret: The client secret for the Sigma Computing API.
            auth_endpoint: The OAuth endpoint for token requests.
            oauth_scopes: Optional OAuth scopes.
        """
        super().__init__(
            auth_endpoint=auth_endpoint,
            client_id=client_id,
            client_secret=client_secret,
            oauth_scopes=oauth_scopes,
        )
        self._token_expires_at: float | None = None

    @property
    @override
    def oauth_request_body(self) -> dict[str, Any]:
        """Return request body for OAuth request.

        Returns:
            Dictionary with OAuth request body.

        """
        return {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
