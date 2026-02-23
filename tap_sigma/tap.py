"""Sigma Computing tap class."""

from __future__ import annotations

import sys

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_sigma import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

try:
    import requests_cache

    requests_cache.install_cache()
except ImportError:
    pass


class TapSigma(Tap):
    """Sigma Computing tap class."""

    name = "tap-sigma"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            secret=True,
            description="Sigma Computing API Client ID",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            secret=True,
            description="Sigma Computing API Client Secret",
        ),
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            description=(
                "Base API URL for your Sigma Computing instance "
                "(e.g., https://aws-api.sigmacomputing.com)"
            ),
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Earliest record date to sync",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        return [
            # Top-level streams
            streams.generic.AccountTypesStream(self),
            streams.generic.ConnectionsStream(self),
            streams.generic.FilesStream(self),
            streams.generic.TagsStream(self),
            streams.generic.TeamsStream(self),
            streams.generic.TemplatesStream(self),
            streams.generic.TranslationFilesStream(self),
            streams.generic.UserAttributesStream(self),
            streams.generic.WorkspacesStream(self),
            # Dataset streams
            streams.datasets.DatasetsStream(self),
            streams.datasets.DatasetGrantsStream(self),
            streams.datasets.DatasetMaterializationsStream(self),
            streams.datasets.DatasetSourcesStream(self),
            # Data Model streams
            streams.data_models.DataModelsStream(self),
            streams.data_models.DataModelElementsStream(self),
            streams.data_models.DatamodelSourcesStream(self),
            streams.data_models.DataModelTagsStream(self),
            streams.data_models.DataModelMaterializationSchedulesStream(self),
            # Member streams
            streams.members.MembersStream(self),
            streams.members.MemberTeamsStream(self),
            # Workbook streams
            streams.workbooks.WorkbooksStream(self),
            streams.workbooks.WorkbookColumnsStream(self),
            streams.workbooks.WorkbookControlsStream(self),
            streams.workbooks.WorkbookElementsStream(self),
            streams.workbooks.WorkbookMaterializationSchedulesStream(self),
            streams.workbooks.WorkbookPagesStream(self),
            streams.workbooks.WorkbookPageElementsStream(self),
            streams.workbooks.WorkbookQueriesStream(self),
            streams.workbooks.WorkbookSchedulesStream(self),
        ]


if __name__ == "__main__":
    TapSigma.cli()
