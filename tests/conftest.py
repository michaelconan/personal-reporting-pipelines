"""Global test configuration for all test modules.

This module sets up common test environment variables and configurations
that are shared across all test suites.
"""

import logging
import os

os.environ["TEST"] = "True"
os.environ["DBT_TARGET"] = "test"
os.environ.setdefault("RUNTIME__LOG_LEVEL", "INFO")

# dlt installs its own logger and formatter; ensure the logger propagates to
# pytest's active console handlers so its INFO/ERROR records appear in test output.
dlt_logger = logging.getLogger("dlt")
dlt_logger.setLevel(logging.INFO)
dlt_logger.propagate = True

for handler in list(dlt_logger.handlers):
    dlt_logger.removeHandler(handler)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
dlt_logger.addHandler(stream_handler)
