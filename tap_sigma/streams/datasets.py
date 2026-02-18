"""Stream definitions for dataset endpoints."""

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
