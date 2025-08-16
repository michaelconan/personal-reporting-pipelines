import os

# This environment variable is set to disable the Google Secrets provider for all dlt unit tests.
# It needs to be set before any dlt modules are imported, which is why it's at the top of this file.
os.environ["PROVIDERS__ENABLE_GOOGLE_SECRETS"] = "false"
os.environ["DLT_TELEMETRY_DISABLED"] = "1"
