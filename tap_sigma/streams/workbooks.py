"""Stream definitions for workbook endpoints."""

from __future__ import annotations

import sys
from importlib import resources
from typing import TYPE_CHECKING

from singer_sdk import SchemaDirectory, StreamSchema

from tap_sigma import schemas as schemas_module
from tap_sigma.client import SigmaStream

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
class WorkbookControlsStream(SigmaStream):
    """Workbook controls stream.

    https://help.sigmacomputing.com/reference/getworkbookcontrols
    """

    name = "workbook_controls"
    path = "/v2/workbooks/{workbookId}/controls"
    primary_keys = ("workbookId", "name")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = WorkbooksStream


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


class WorkbookQueriesStream(SigmaStream):
    """Workbook queries stream."""

    name = "workbook_queries"
    path = "/v2/workbooks/{workbookId}/queries"
    primary_keys = ("workbookId", "queryId")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = WorkbooksStream


class WorkbookSchedulesStream(SigmaStream):
    """Workbook schedules stream."""

    name = "workbook_schedules"
    path = "/v2/workbooks/{workbookId}/schedules"
    primary_keys = ("workbookId", "scheduledNotificationId")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = WorkbooksStream
