"""REST client handling for Sigma Computing API streams."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urljoin

from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams import RESTStream

from tap_sigma.auth import SigmaAuthenticator

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context


class SigmaPaginator(BaseAPIPaginator[int]):
    """Paginator for Sigma Computing API."""

    @override
    def __init__(self, **kwargs: Any) -> None:
        """Initialize paginator."""
        kwargs.setdefault("start_value", 1)  # always start at page 1
        super().__init__(**kwargs)

    @override
    def get_next(self, response: requests.Response) -> int | None:
        """Get next page number."""
        next_page = response.json().get("nextPage")
        return int(next_page) if next_page else None


class SigmaStream(RESTStream):
    """Base stream class for Sigma Computing API."""

    records_jsonpath = "$.entries[*]"

    @property
    @override
    def url_base(self) -> str:
        """Return the base URL for the API."""
        return self.config.get("api_url", "").rstrip("/")

    @property
    @override
    def authenticator(self) -> SigmaAuthenticator:
        """Return shared authenticator instance to avoid rate limiting."""
        return SigmaAuthenticator(
            client_id=self.config["client_id"],
            client_secret=self.config["client_secret"],
            auth_endpoint=urljoin(self.url_base, "/v2/auth/token"),
        )

    @override
    def get_new_paginator(self) -> BaseAPIPaginator:
        """Get a new paginator."""
        return SigmaPaginator()

    @override
    def get_url_params(
        self,
        context: Context | None = None,
        next_page_token: int | None = None,
    ) -> dict[str, Any]:
        """Get URL parameters."""
        params = cast(
            "dict[str, Any]",
            super().get_url_params(context, next_page_token),
        )
        params["page"] = next_page_token
        params["limit"] = 1000
        return params
