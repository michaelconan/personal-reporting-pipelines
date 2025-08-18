import os
import json
import pytest

# This environment variable is set to disable the Google Secrets provider for all dlt unit tests.
# It needs to be set before any dlt modules are imported, which is why it's at the top of this file.
os.environ["PROVIDERS__ENABLE_GOOGLE_SECRETS"] = "false"
os.environ["DLT_TELEMETRY_DISABLED"] = "1"


MOCK_FOLDER = "tests/mock_data"


def sample_data(file_name: str, fallback: str = None) -> dict:
    file = os.path.join(MOCK_FOLDER, file_name)
    if not os.path.exists(file) and fallback is not None:
        file = os.path.join(MOCK_FOLDER, fallback)
    with open(file, "r") as f:
        return json.load(f)


def sample_response(file_name: str) -> tuple[int, dict, str]:
    with open(os.path.join(MOCK_FOLDER, file_name), "r") as f:
        return (200, {}, f.read())
