# 1Password template for Databricks environment variables
# Run: op inject -f -i .env.databricks.tpl -o .env.databricks
# Then: source .env.databricks

DATABRICKS_HOST="{{op://reporting/databricks/hostname}}"
DATABRICKS_HTTP_PATH="{{op://reporting/databricks/warehouse}}"
DATABRICKS_CLIENT_ID="{{op://reporting/databricks/username}}"
DATABRICKS_CLIENT_SECRET="{{op://reporting/databricks/credential}}"
DATABRICKS_CATALOG="{{op://reporting/databricks/catalog}}"