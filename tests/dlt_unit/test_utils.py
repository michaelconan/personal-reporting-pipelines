"""
Unit tests for common utility functions.
"""

# Base imports
import os
from unittest.mock import patch

# Local imports
from pipelines.common.utils import (
    should_force_full_refresh,
    get_refresh_mode,
)


def test_should_force_full_refresh():
    """
    Test the should_force_full_refresh function.
    """
    with patch.dict(os.environ, {"FORCE_FULL_REFRESH": "true"}):
        assert should_force_full_refresh() is True

    with patch.dict(os.environ, {"FORCE_FULL_REFRESH": "1"}):
        assert should_force_full_refresh() is True

    with patch.dict(os.environ, {"FORCE_FULL_REFRESH": "yes"}):
        assert should_force_full_refresh() is True

    with patch.dict(os.environ, {"FORCE_FULL_REFRESH": "false"}):
        assert should_force_full_refresh() is False

    with patch.dict(os.environ, {"FORCE_FULL_REFRESH": "0"}):
        assert should_force_full_refresh() is False

    with patch.dict(os.environ, {"FORCE_FULL_REFRESH": "no"}):
        assert should_force_full_refresh() is False

    with patch.dict(os.environ, {}, clear=True):
        assert should_force_full_refresh() is False


def test_get_refresh_mode():
    """
    Test the get_refresh_mode function.
    """
    # Test with global full refresh
    with patch.dict(os.environ, {"FORCE_FULL_REFRESH": "true"}):
        assert get_refresh_mode() is False

    # Test with pipeline-specific full refresh
    with patch.dict(
        os.environ,
        {"PIPELINE_NAME": "TEST_PIPELINE", "TEST_PIPELINE_FULL_REFRESH": "true"},
    ):
        assert get_refresh_mode() is False

    # Test with no environment variables and default incremental
    with patch.dict(os.environ, {}, clear=True):
        assert get_refresh_mode(default_incremental=True) is True

    # Test with no environment variables and default full refresh
    with patch.dict(os.environ, {}, clear=True):
        assert get_refresh_mode(default_incremental=False) is False
