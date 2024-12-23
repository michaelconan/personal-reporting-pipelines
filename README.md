# personal-reporting

Airflow server for personal data integration and experimentation.

## Overview

Dev container, requirements and constraints files used for local development prior to deployment.

## Data Sources

1. Google Contacts
2. HubSpot
3. Notion

## Airflow Notes

- DAGs should contain a number of related steps (e.g., extract-transform-load)
- DAGs can be linked to one another via datasets to enable triggering and dependency graph

## Airflow Setup

1. Started with official [docker compose](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
2. Switched to `LocalExecutor` and trimmed down to webserver + scheduler image and Postgres for local testing

## Azure Setup

I have used [this quickstart](https://github.com/savjani-zz/azure-quickstart-templates/tree/master/101-webapp-linux-airflow-postgresql) for reference and the idea to run the webserver and scheduler together in a single container with `LocalExecutor`. `CeleryExecutor` shouldn't be necessary unless scale increases and multiple workers are required.

1. Create container registry and enable [admin user access](https://learn.microsoft.com/en-us/azure/container-registry/container-registry-authentication?tabs=azure-cli#admin-account)
   `az acr update -n airflowreportingregistry --admin-enabled true`
2. Push container images to registry using Docker or automated pipeline (Azure DevOps)
3. Create App Service app with container and connected services (Postgres Database and Redis Cache)