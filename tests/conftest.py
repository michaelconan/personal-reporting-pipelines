"""Global test configuration for all test modules.

This module sets up common test environment variables and configurations
that are shared across all test suites.
"""

import os

os.environ["TEST"] = "True"
os.environ["DBT_TARGET"] = "test"
