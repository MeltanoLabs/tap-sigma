"""Tests for tap-sigma core functionality."""

import os

import requests
from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_sigma.client import SigmaPaginator
from tap_sigma.tap import TapSigma

CI = os.getenv("GITHUB_ACTIONS", "false") == "true"

# Configuration for testing
SAMPLE_CONFIG = {
    "api_url": "https://aws-api.sigmacomputing.com",
}


# Run standard tap tests from the SDK
TestTapSigma = get_tap_test_class(
    tap_class=TapSigma,
    config=SAMPLE_CONFIG,
    suite_config=SuiteConfig(
        ignore_no_records_for_streams=[
            "dataset_materializations",
            "tags",
            "translation_files",
        ],
    ),
    include_tap_tests=not CI,
    include_stream_tests=not CI,
    include_stream_attribute_tests=not CI,
)


class TestSigmaPaginator:
    """Test the Sigma paginator."""

    def test_pagination(self) -> None:
        """Test the has_more method."""
        paginator = SigmaPaginator()
        response = requests.Response()

        response._content = b'{"nextPage": 2}'  # noqa: SLF001
        paginator.advance(response)
        assert paginator.current_value == 2  # noqa: PLR2004
        assert not paginator.finished

        response._content = b'{"nextPage": null}'  # noqa: SLF001
        paginator.advance(response)
        assert paginator.current_value == 2  # noqa: PLR2004
        assert paginator.finished
