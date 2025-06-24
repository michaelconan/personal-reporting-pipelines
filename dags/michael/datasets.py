"""
datasets.py

Common definitions of Airflow datasets for triggering DAGs.
"""

from airflow.datasets import Dataset

# Raw datasets used for airflow DAG scheduling

# Notion Datasets
NOTION_DS = Dataset("notion_habits")

# Hubspot Datasets
HUBSPOT_DS = Dataset("hubspot_crm")
HUBSPOT_CONTACTS_DS = Dataset("hubspot_contacts")
HUBSPOT_COMPANIES_DS = Dataset("hubspot_companies")
HUBSPOT_ENGAGEMENTS_DS = Dataset("hubspot_engagements")

# Fitbit Datasets
FITBIT_SLEEP_DS = Dataset("fitbit_sleep")

# All datasets for DBT triggering
RAW_DATASETS = [
    NOTION_DS,
    HUBSPOT_DS,
    FITBIT_SLEEP_DS,
]
