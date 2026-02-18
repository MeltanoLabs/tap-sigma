"""Stream definitions for member endpoints."""

from __future__ import annotations

import sys
from importlib import resources
from typing import TYPE_CHECKING, Any

from singer_sdk import SchemaDirectory, StreamSchema

from tap_sigma import schemas as schemas_module
from tap_sigma.client import SigmaStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context, Record


SCHEMAS = SchemaDirectory(resources.files(schemas_module))


class MembersStream(SigmaStream):
    """Members stream."""

    name = "members"
    path = "/v2/members"
    primary_keys = ("memberId",)
    replication_key = None
    schema = StreamSchema(SCHEMAS)

    @override
    def get_url_params(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """Get URL parameters."""
        return {
            **super().get_url_params(*args, **kwargs),
            "includeArchived": "true",
            "includeInactive": "true",
        }

    @override
    def generate_child_contexts(
        self,
        record: Record,
        context: Context | None,
    ) -> Iterable[Context | None]:
        yield {"memberId": record["memberId"]}


# Member child streams
class MemberTeamsStream(SigmaStream):
    """Member teams stream.

    https://help.sigmacomputing.com/reference/listmemberteams
    """

    name = "member_teams"
    # https://api.sigmacomputing.com/v2/members/{memberId}/teams
    path = "/v2/members/{memberId}/teams"
    primary_keys = ("memberId", "teamId")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = MembersStream
