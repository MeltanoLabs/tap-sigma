"""Stream definitions for workbook endpoints."""

from __future__ import annotations

import sys
from importlib import resources
from typing import TYPE_CHECKING, Any, ClassVar

from singer_sdk import SchemaDirectory, StreamSchema

from tap_sigma import schemas as schemas_module
from tap_sigma.client import SigmaChildStream, SigmaStream, SigmaStringPagePaginator

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record

SCHEMAS = SchemaDirectory(resources.files(schemas_module))


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
        if record.get("isArchived"):  # Skip archived workbooks
            return None

        return {"workbookId": record["workbookId"]}


# Workbook child streams
class WorkbookColumnsStream(SigmaChildStream):
    """Workbook columns stream.

    https://help.sigmacomputing.com/reference/getworkbookcolumns
    """

    name = "workbook_columns"
    path = "/v2/workbooks/{workbookId}/columns"
    primary_keys = ("workbookId", "elementId", "columnId")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = WorkbooksStream

    @override
    def get_new_paginator(self) -> SigmaStringPagePaginator:
        """Get a new paginator."""
        return SigmaStringPagePaginator(start_value=None)


class WorkbookControlsStream(SigmaChildStream):
    """Workbook controls stream.

    https://help.sigmacomputing.com/reference/getworkbookcontrols
    """

    name = "workbook_controls"
    path = "/v2/workbooks/{workbookId}/controls"
    primary_keys = ("workbookId", "name")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = WorkbooksStream


class WorkbookElementsStream(SigmaChildStream):
    """Workbook elements stream.

    https://help.sigmacomputing.com/reference/listworkbookelements
    """

    name = "workbook_elements"
    path = "/v2/workbooks/{workbookId}/elements"
    primary_keys = ("workbookId", "elementId")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = WorkbooksStream


class WorkbookLineageStream(SigmaStream):
    """Workbook sources stream."""

    name = "workbook_lineage"
    path = "/v2/workbooks/{workbookId}/lineage"
    primary_keys = ("workbookId", "_sdc_source_id")
    replication_key = None
    parent_stream_type = WorkbooksStream

    schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            # Element details
            "sourceIds": {
                "type": ["array", "null"],
                "items": {"type": "string"},
                "description": "List of direct sources for the element. Either element IDs or data source IDs.",  # noqa: E501
            },
            "elementId": {
                "type": ["string", "null"],
                "description": "Unique identifier of the element",
            },
            "dataSourceIds": {
                "type": ["array", "null"],
                "items": {"type": "string"},
                "description": "List of root sources for the element as data source IDs.",
            },
            # Source data model details
            "connectionId": {"type": ["string", "null"]},
            "name": {"type": ["string", "null"]},
            "dataModelId": {"type": ["string", "null"]},
            # Source table or dataset details
            "inodeId": {"type": ["string", "null"]},
            # Source custom SQL details
            "definition": {"type": ["string", "null"]},
            # Metadata
            "workbookId": {"type": "string"},
            "_sdc_source_id": {"type": "string"},
        },
    }

    @override
    def post_process(self, row: Record, context: Context | None = None) -> Record | None:
        if element_id := row.pop("elementId", None):
            row["_sdc_source_id"] = element_id
        if data_model_id := row.pop("dataModelId", None):
            row["_sdc_source_id"] = data_model_id
        if inode_id := row.pop("inodeId", None):
            row["_sdc_source_id"] = inode_id
        if definition := row.pop("definition", None):
            row["_sdc_source_id"] = definition

        return row


class WorkbookMaterializationSchedulesStream(SigmaChildStream):
    """Workbook materialization schedules stream."""

    name = "workbook_materialization_schedules"
    path = "/v2/workbooks/{workbookId}/materialization-schedules"
    primary_keys = ("workbookId", "sheetId")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = WorkbooksStream


class WorkbookPagesStream(SigmaChildStream):
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


class WorkbookPageElementsStream(SigmaChildStream):
    """Workbook page elements stream."""

    name = "workbook_page_elements"
    path = "/v2/workbooks/{workbookId}/pages/{pageId}/elements"
    primary_keys = ("workbookId", "pageId", "elementId")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = WorkbookPagesStream


class WorkbookQueriesStream(SigmaChildStream, default_page_size=50):
    """Workbook queries stream."""

    name = "workbook_queries"
    path = "/v2/workbooks/{workbookId}/queries"
    primary_keys = ("workbookId", "elementId")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = WorkbooksStream

    @override
    def get_new_paginator(self) -> SigmaStringPagePaginator:
        """Get a new paginator."""
        return SigmaStringPagePaginator(start_value=None)


class WorkbookSchedulesStream(SigmaChildStream):
    """Workbook schedules stream."""

    name = "workbook_schedules"
    path = "/v2/workbooks/{workbookId}/schedules"
    primary_keys = ("workbookId", "scheduledNotificationId")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = WorkbooksStream


class WorkbookSourcesStream(SigmaChildStream):
    """Workbook sources stream.

    https://help.sigmacomputing.com/reference/getworkbooksources
    """

    name = "workbook_sources"
    path = "/v2/workbooks/{workbookId}/sources"
    primary_keys = ("workbookId", "_sdc_source_id")
    replication_key = None
    parent_stream_type = WorkbooksStream

    schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {
            "type": {"type": ["string", "null"]},
            # Source is a data model
            "sourceDataModelId": {"type": ["string", "null"]},
            "elementIds": {
                "type": ["array", "null"],
                "items": {"type": "string"},
                "description": "IDs of elements that make up this data model's sources.",
                "title": "Source element IDs",
            },
            "versionTagId": {"type": ["string", "null"]},
            # Source is a dataset
            "sourceDatasetId": {"type": ["string", "null"]},
            # Source is a table
            "sourceTableId": {"type": ["string", "null"]},
            # Metadata
            "workbookId": {"type": "string"},
            "_sdc_source_id": {"type": "string"},
        },
    }

    @override
    def post_process(self, row: Record, context: Context | None = None) -> Record | None:
        source_type = row["type"]
        if data_model_id := row.pop("dataModelId", None):
            row["sourceDataModelId"] = data_model_id
            row["_sdc_source_id"] = data_model_id
        if inode_id := row.pop("inodeId", None):
            key = "sourceDatasetId" if source_type == "dataset" else "sourceTableId"
            row[key] = inode_id
            row["_sdc_source_id"] = inode_id

        return row

    @override
    def get_url_params(
        self,
        context: Context | None = None,
        next_page_token: int | None = None,
    ) -> dict[str, Any]:
        return {}
