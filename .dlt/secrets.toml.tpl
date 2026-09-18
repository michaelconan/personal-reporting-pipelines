# 1Password template config file for dlt sources
[sources.notion]
api_key="{{op://reporting/notion/credential}}"

[sources.hubspot]
api_key="{{op://reporting/hubspot/credential}}"

[sources.google_health]
client_id="{{op://reporting/google-health/username}}"
client_secret="{{op://reporting/google-health/credential}}"
refresh_token="{{op://reporting/google-health/refresh_token}}"

[destination.databricks.credentials]
server_hostname = "{{op://reporting/databricks/hostname}}"
http_path = "{{op://reporting/databricks/warehouse}}"
client_id = "{{op://reporting/databricks/username}}"
client_secret = "{{op://reporting/databricks/credential}}"
catalog = "{{op://reporting/databricks/catalog}}"
