"""Stream definitions for data model endpoints."""

from __future__ import annotations

import sys
from importlib import resources
from typing import TYPE_CHECKING, Any, ClassVar

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


class DataModelsStream(SigmaStream):
    """Data models stream."""

    name = "data_models"
    path = "/v2/dataModels"
    primary_keys = ("dataModelId",)
    replication_key = None
    schema = StreamSchema(SCHEMAS)

    @override
    def get_child_context(
        self,
        record: Record,
        context: Context | None = None,
    ) -> Context | None:
        """Return context for child streams."""
        return {"_sdc_data_model_id": record["dataModelId"]}


# Data Model child streams
class DatamodelSourcesStream(SigmaStream):
    """Dataset sources stream."""

    name = "data_model_sources"
    path = "/v2/dataModels/{_sdc_data_model_id}/sources"
    primary_keys = ("_sdc_data_model_id", "_sdc_source_id")
    parent_stream_type = DataModelsStream

    next_page_token_jsonpath = "$.nextPageToken"  # noqa: S105

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
            "_sdc_data_model_id": {"type": "string"},
            "_sdc_source_id": {"type": "string"},
        },
    }

    @override
    def post_process(self, row: Record, context: Context | None = None) -> Record | None:
        if data_model_id := row.pop("dataModelId", None):
            row["sourceDataModelId"] = data_model_id
            row["_sdc_source_id"] = data_model_id
        if dataset_id := row.pop("datasetId", None):
            row["sourceDatasetId"] = dataset_id
            row["_sdc_source_id"] = dataset_id
        if table_id := row.pop("tableId", None):
            row["sourceTableId"] = table_id
            row["_sdc_source_id"] = table_id

        return row


class DataModelTagsStream(SigmaStream):
    """Data model tags stream."""

    name = "data_model_tags"
    path = "/v2/dataModels/{_sdc_data_model_id}/tags"
    primary_keys = ("versionTagId",)
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = DataModelsStream


class DataModelMaterializationSchedulesStream(SigmaStream):
    """Data model materialization schedules stream.

    https://help.sigmacomputing.com/reference/listdatamodelmaterializationschedules
    """

    name = "data_model_materialization_schedules"
    path = "/v2/dataModels/{_sdc_data_model_id}/materializationSchedules"
    primary_keys = ("_sdc_data_model_id", "sheetId")
    replication_key = None
    schema = StreamSchema(SCHEMAS)
    parent_stream_type = DataModelsStream
