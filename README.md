# personal-reporting-airflow

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
2. Decided to [extend the image](https://airflow.apache.org/docs/docker-stack/build.html#extending-the-image) starting with the slim version due to limited packaged requirements
3. Switched to `LocalExecutor` 
4. Stripped down the Docker Compose to a single container to run webserver + scheduler and use for dev container

## Azure Setup

I have used [this quickstart](https://github.com/savjani-zz/azure-quickstart-templates/tree/master/101-webapp-linux-airflow-postgresql) for reference and the idea to run the webserver and scheduler together in a single container with `LocalExecutor`. `CeleryExecutor` shouldn't be necessary unless scale increases and multiple workers are required.

1. Create PostgreSQL flexible server for database
2. Create App Service app with container (NOTE: I followed [this tutorial](https://learn.microsoft.com/en-us/azure/app-service/configure-custom-container?tabs=debian&pivots=container-linux#enable-ssh) to configure SSH access in app service)

# Automated Deployment

1. Referenced [this workflow](https://docs.github.com/en/actions/use-cases-and-examples/publishing-packages/publishing-docker-images#publishing-images-to-github-packages) to build and publish to GitHub Container Registry
2. Referenced [this workflow](https://learn.microsoft.com/en-us/azure/app-service/deploy-best-practices#use-github-actions) to deploy updated image to Azure Web App