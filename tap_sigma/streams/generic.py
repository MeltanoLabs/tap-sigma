"""Stream definitions for Sigma Computing API."""

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
    from singer_sdk.helpers.types import Context


SCHEMAS = SchemaDirectory(resources.files(schemas_module))


class AccountTypesStream(SigmaStream):
    """Account types stream."""

    name = "account_types"
    path = "/v2/accountTypes"
    next_page_token_jsonpath = "$.nextPageToken"  # noqa: S105
    primary_keys = ("accountTypeId",)
    replication_key = None
    schema = StreamSchema(SCHEMAS)

    @override
    def get_url_params(  # ty:ignore[invalid-method-override]
        self,
        context: Context | None = None,
        next_page_token: str | None = None,  # type: ignore[override]
    ) -> dict[str, Any]:
        return {
            "pageToken": next_page_token,
            "pageSize": 1000,
        }


class ConnectionsStream(SigmaStream):
    """Connections stream."""

    name = "connections"
    path = "/v2/connections"
    primary_keys = ("connectionId",)
    replication_key = None
    schema = StreamSchema(SCHEMAS)


class TeamsStream(SigmaStream):
    """Teams stream."""

    name = "teams"
    path = "/v2/teams"
    primary_keys = ("teamId",)
    replication_key = None
    schema = StreamSchema(SCHEMAS)


class FilesStream(SigmaStream):
    """Files stream."""

    name = "files"
    path = "/v2/files"
    primary_keys = ("id",)  # Use 'id' which is actually returned by the API
    replication_key = None
    schema = StreamSchema(SCHEMAS)


class TagsStream(SigmaStream):
    """Tags stream."""

    name = "tags"
    path = "/v2/tags"
    primary_keys = ("tagId",)
    replication_key = None
    schema = StreamSchema(SCHEMAS)


class UserAttributesStream(SigmaStream):
    """User attributes stream."""

    name = "user_attributes"
    path = "/v2/user-attributes"
    primary_keys = ("userAttributeId",)
    replication_key = None
    schema = StreamSchema(SCHEMAS)


class WorkspacesStream(SigmaStream):
    """Workspaces stream."""

    name = "workspaces"
    path = "/v2/workspaces"
    primary_keys = ("workspaceId",)
    replication_key = None
    schema = StreamSchema(SCHEMAS)


class TemplatesStream(SigmaStream):
    """Templates stream."""

    name = "templates"
    path = "/v2/templates"
    primary_keys = ("templateId",)
    replication_key = None
    schema = StreamSchema(SCHEMAS)


class TranslationFilesStream(SigmaStream):
    """Translation files stream."""

    name = "translation_files"
    path = "/v2/translations/organization"
    primary_keys = ("lng",)
    replication_key = None
    schema = StreamSchema(SCHEMAS)
