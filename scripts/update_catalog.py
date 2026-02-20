"""Update the OpenAPI schema from the Sigma API."""  # noqa: INP001

from __future__ import annotations

import http
import json
import logging
import pathlib
import sys
from typing import Any

import urllib3
from singer_sdk.schema.source import OpenAPISchemaNormalizer
from toolz import get_in

OPENAPI_URL = "https://help.sigmacomputing.com/openapi/sigma-computing-public-rest-api.json"
SCHEMAS_PATH = "tap_sigma/schemas"

logging.basicConfig(format="%(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger()


def _get_schema_path(
    endpoint: str,
    *,
    tail: tuple[str | int, ...] = ("allOf", 0, "properties", "entries", "items"),
) -> list[str | int]:
    """Get the path to a schema in the OpenAPI spec."""
    return [
        "paths",
        endpoint,
        "get",
        "responses",
        "200",
        "content",
        "application/json",
        "schema",
        *tail,
    ]


def _merge_all_of(*schemas: dict[str, Any]) -> dict[str, Any]:
    """Merge all of schemas into a single schema.

    - `properties` are merged into a single dict
    - `required` is merged into a single list

    Args:
        *schemas: The schemas to merge.

    Returns:
        The merged schema.
    """
    result: dict[str, Any] = {
        "type": "object",
        "properties": {},
        "required": [],
    }
    for schema in schemas:
        result["properties"] |= schema["properties"]
        result["required"] = sorted(set(result["required"] + schema.get("required", [])))

    return result


def extract_schemas(spec: dict[str, Any]) -> dict[str, dict[str, Any]]:  # noqa: PLR0915
    """Extract schemas for all streams from the OpenAPI spec.

    Args:
        spec: The OpenAPI specification.

    Returns:
        A dictionary mapping stream names to their schemas.
    """
    schemas = {}

    # Account types
    schemas["account_types"] = get_in(
        _get_schema_path("/v2/accountTypes"),
        spec,
    )

    # Connections
    schema = get_in(
        _get_schema_path(
            "/v2/connections",
            tail=("allOf", 0, "properties", "entries", "items", "allOf"),
        ),
        spec,
    )
    merged = _merge_all_of(*schema)
    merged["properties"]["host"] = {"type": ["string", "null"]}
    schemas["connections"] = merged

    # Data models
    schemas["data_models"] = get_in(
        _get_schema_path("/v2/dataModels"),
        spec,
    )

    # Datasets
    schemas["datasets"] = get_in(
        _get_schema_path("/v2/datasets"),
        spec,
    )

    # Files
    schema = get_in(
        _get_schema_path(
            "/v2/files",
            tail=("allOf", 0, "properties", "entries", "items", "oneOf", 0, "allOf"),
        ),
        spec,
    )
    schemas["files"] = _merge_all_of(*schema)

    # Members
    schemas["members"] = get_in(
        _get_schema_path(
            "/v2/members",
            tail=("oneOf", 1, "properties", "entries", "items"),
        ),
        spec,
    )

    # Teams
    schemas["teams"] = get_in(
        _get_schema_path(
            "/v2/teams",
            tail=("oneOf", 1, "properties", "entries", "items"),
        ),
        spec,
    )

    # Tags
    schema = get_in(
        _get_schema_path(
            "/v2/tags",
            tail=("allOf", 0, "properties", "entries", "items", "allOf"),
        ),
        spec,
    )
    schemas["tags"] = _merge_all_of(*schema)

    # Templates
    schema = get_in(
        _get_schema_path(
            "/v2/templates",
            tail=("allOf", 0, "properties", "entries", "items", "allOf"),
        ),
        spec,
    )
    schemas["templates"] = _merge_all_of(*schema)

    # Translation files
    schemas["translation_files"] = get_in(
        _get_schema_path("/v2/translations/organization"),
        spec,
    )

    # User attributes
    schema = get_in(
        _get_schema_path(
            "/v2/user-attributes",
            tail=("allOf", 0, "properties", "entries", "items", "allOf"),
        ),
        spec,
    )
    schemas["user_attributes"] = _merge_all_of(*schema)

    # Workbooks
    schema = get_in(
        _get_schema_path(
            "/v2/workbooks",
            tail=("allOf", 0, "properties", "entries", "items", "allOf"),
        ),
        spec,
    )
    schemas["workbooks"] = _merge_all_of(*schema)

    # Workspaces
    schemas["workspaces"] = get_in(
        _get_schema_path(
            "/v2/workspaces",
            tail=("oneOf", 1, "properties", "entries", "items"),
        ),
        spec,
    )

    # Dataset child streams
    schema = get_in(
        _get_schema_path("/v2/datasets/{datasetId}/grants"),
        spec,
    )
    schema["properties"]["datasetId"] = {"type": "string"}
    schema["properties"]["permission"] = {"type": "string"}
    schemas["dataset_grants"] = schema

    schema = get_in(
        _get_schema_path("/v2/datasets/{datasetId}/materialization"),
        spec,
    )
    schema["properties"]["datasetId"] = {"type": "string"}
    schemas["dataset_materializations"] = schema

    schema = get_in(
        _get_schema_path("/v2/datasets/{datasetId}/sources", tail=("items",)),
        spec,
    )
    schema["properties"]["datasetId"] = {"type": "string"}
    schemas["dataset_sources"] = schema

    # Data Model child streams
    schema = get_in(
        _get_schema_path("/v2/dataModels/{dataModelId}/tags"),
        spec,
    )
    schema["properties"]["_sdc_data_model_id"] = {"type": "string"}
    schemas["data_model_tags"] = schema

    # https://help.sigmacomputing.com/reference/listdatamodelmaterializationschedules
    schema = get_in(
        _get_schema_path("/v2/dataModels/{dataModelId}/materializationSchedules"),
        spec,
    )
    schema["properties"]["_sdc_data_model_id"] = {"type": "string"}
    schemas["data_model_materialization_schedules"] = schema

    # Member child streams
    # https://help.sigmacomputing.com/reference/listmemberteams
    schema = get_in(
        _get_schema_path(
            "/v2/members/{memberId}/teams",
            tail=("allOf", 0, "properties", "entries", "items", "allOf"),
        ),
        spec,
    )
    merged = _merge_all_of(*schema)
    merged["properties"]["memberId"] = {"type": "string"}
    schemas["member_teams"] = merged

    # Workbook child streams
    # https://help.sigmacomputing.com/reference/getworkbookcontrols
    schema = get_in(
        _get_schema_path("/v2/workbooks/{workbookId}/controls"),
        spec,
    )
    schema["properties"]["workbookId"] = {"type": "string"}
    schema["properties"]["valueType"] = {"type": ["string", "null"]}
    schemas["workbook_controls"] = schema

    schema = get_in(
        _get_schema_path(
            "/v2/workbooks/{workbookId}/materialization-schedules",
            tail=("oneOf", 1, "properties", "entries", "items"),
        ),
        spec,
    )
    schema["properties"]["workbookId"] = {"type": "string"}
    schemas["workbook_materialization_schedules"] = schema

    schema = get_in(
        _get_schema_path(
            "/v2/workbooks/{workbookId}/pages",
            tail=("allOf", 0, "properties", "entries", "items", "allOf"),
        ),
        spec,
    )
    merged = _merge_all_of(*schema)
    merged["properties"]["workbookId"] = {"type": "string"}
    schemas["workbook_pages"] = merged

    schema = get_in(
        _get_schema_path(
            "/v2/workbooks/{workbookId}/pages/{pageId}/elements",
            tail=("allOf", 0, "properties", "entries", "items", "allOf"),
        ),
        spec,
    )
    merged = _merge_all_of(*schema)
    merged["properties"]["workbookId"] = {"type": "string"}
    merged["properties"]["pageId"] = {"type": "string"}
    schemas["workbook_page_elements"] = merged

    schema = get_in(
        _get_schema_path("/v2/workbooks/{workbookId}/queries"),
        spec,
    )
    schemas["workbook_queries"] = schema

    schema = get_in(
        _get_schema_path(
            "/v2/workbooks/{workbookId}/schedules",
            tail=("oneOf", 1, "properties", "entries", "items", "allOf"),
        ),
        spec,
    )
    merged = _merge_all_of(*schema)
    merged["properties"]["workbookId"] = {"type": "string"}
    schemas["workbook_schedules"] = merged

    return schemas


def main() -> None:
    """Update the OpenAPI schema from the Sigma API."""
    logger.info("Updating OpenAPI schema from %s", OPENAPI_URL)
    response = urllib3.request("GET", OPENAPI_URL)
    if response.status != http.HTTPStatus.OK:
        logger.error("Failed to fetch OpenAPI spec: %s", response.reason)
        sys.exit(1)

    spec = response.json()

    # Extract and save individual schemas
    logger.info("Extracting schemas from OpenAPI spec")
    schemas = extract_schemas(spec)

    # Create schemas directory if it doesn't exist
    schemas_dir = pathlib.Path(SCHEMAS_PATH)
    schemas_dir.mkdir(exist_ok=True)
    logger.info("Created/verified schemas directory at %s", SCHEMAS_PATH)

    normalizer = OpenAPISchemaNormalizer()

    # Save each schema
    for stream_name, schema in schemas.items():
        schema_file = schemas_dir / f"{stream_name}.json"
        normalized_schema = normalizer.normalize_schema(schema)
        schema_content = json.dumps(normalized_schema, indent=2) + "\n"
        schema_file.write_text(schema_content, encoding="utf-8")
        logger.info("Saved schema for %s", stream_name)

    logger.info("Successfully updated %d schemas", len(schemas))


if __name__ == "__main__":
    main()
