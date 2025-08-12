import os

# This environment variable is set to disable the Google Secrets provider for all tests.
# It needs to be set before any dlt modules are imported, which is why it's at the top of this file.
os.environ["PROVIDERS__ENABLE_GOOGLE_SECRETS"] = "false"
os.environ["TEST"] = "True"
os.environ["DBT_TARGET"] = "test"
