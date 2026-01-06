"""Stream definitions for Sigma Computing API."""

from __future__ import annotations

import sys
from importlib import resources
from typing import TYPE_CHECKING, Any

from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.pagination import SinglePagePaginator

from tap_sigma import schemas as schemas_module
from tap_sigma.client import SigmaStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record


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


class DatasetsStream(SigmaStream):
    """Datasets stream."""

    name = "datasets"
    path = "/v2/datasets"
    primary_keys = ("datasetId",)
    replication_key = None
    schema = StreamSchema(SCHEMAS)

    @override
    def get_child_context(
        self,
        record: Record,
        context: Context | None = None,
    ) -> Context | None:
        """Return context for child streams."""
        return {"datasetId": record["datasetId"]}


class DataModelsStream(SigmaStream):
    """Data models stream."""

    name = "data_models"
    path = "/v2/dataModels"
    primary_keys = ("dataModelId",)
    replication_key = None
    schema = StreamSchema(SCHEMAS)


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


class WorkbooksStream(SigmaStream):
    """Workbooks stream."""

    name = "workbooks"
    path = "/v2/workbooks"
    primary_keys = ("workbookId",)
    replication_key = None
    schema = StreamSchema(SCHEMAS)

    @override
    def get_child_context(
        self,
        record: Record,
        context: Context | None = None,
    ) -> Context | None:
        """Return context for child streams."""
        _ = context  # Unused
        return {"workbookId": record["workbookId"]}


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


# Dataset child streams
class DatasetMaterializationsStream(SigmaStream):
    """Dataset materializations stream."""

    name = "dataset_materializations"
    path = "/v2/datasets/{datasetId}/materialization"
    primary_keys = ("datasetId", "finishedAt")
    replication_key = None
    parent_stream_type = DatasetsStream
    schema = StreamSchema(SCHEMAS)


class DatasetGrantsStream(SigmaStream):
    """Dataset grants stream."""

    name = "dataset_grants"
    path = "/v2/datasets/{datasetId}/grants"
    primary_keys = ("datasetId", "grantId")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = DatasetsStream


class DatasetSourcesStream(SigmaStream):
    """Dataset sources stream."""

    name = "dataset_sources"
    records_jsonpath = "$[*]"
    path = "/v2/datasets/{datasetId}/sources"
    primary_keys = ("datasetId", "inodeId")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = DatasetsStream

    @override
    def get_new_paginator(self) -> SinglePagePaginator:
        """Get a new paginator."""
        return SinglePagePaginator()

    @override
    def get_url_params(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """Get URL parameters."""
        return {}  # This endpoint expects empty NO QUERY PARAMS


# Workbook child streams
class WorkbookMaterializationSchedulesStream(SigmaStream):
    """Workbook materialization schedules stream."""

    name = "workbook_materialization_schedules"
    path = "/v2/workbooks/{workbookId}/materialization-schedules"
    primary_keys = ("workbookId", "sheetId")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = WorkbooksStream


class WorkbookPagesStream(SigmaStream):
    """Workbook pages stream (child of workbooks)."""

    name = "workbook_pages"
    path = "/v2/workbooks/{workbookId}/pages"
    primary_keys = ("workbookId", "pageId")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = WorkbooksStream

    @override
    def get_child_context(
        self,
        record: Record,
        context: Context | None = None,
    ) -> Context | None:
        """Return context for child streams (page elements)."""
        return {
            "workbookId": record["workbookId"],
            "pageId": record["pageId"],
        }


class WorkbookPageElementsStream(SigmaStream):
    """Workbook page elements stream."""

    name = "workbook_page_elements"
    path = "/v2/workbooks/{workbookId}/pages/{pageId}/elements"
    primary_keys = ("workbookId", "pageId", "elementId")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = WorkbookPagesStream


class WorkbookSchedulesStream(SigmaStream):
    """Workbook schedules stream."""

    name = "workbook_schedules"
    path = "/v2/workbooks/{workbookId}/schedules"
    primary_keys = ("workbookId", "scheduledNotificationId")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = WorkbooksStream
