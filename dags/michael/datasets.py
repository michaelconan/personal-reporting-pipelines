"""
datasets.py

Common definitions of Airflow datasets for triggering DAGs.
"""

from airflow.datasets import Dataset

# Datasets for triggering

# Notion Datasets
NOTION_DAILY_HABITS_DS = Dataset("notion_daily_habits")
NOTION_WEEKLY_HABITS_DS = Dataset("notion_weekly_habits")
NOTION_MONTHLY_HABITS_DS = Dataset("notion_monthly_habits")

# Hubspot Datasets
HUBSPOT_CONTACTS_DS = Dataset("hubspot_contacts")
HUBSPOT_COMPANIES_DS = Dataset("hubspot_companies")
HUBSPOT_ENGAGEMENTS_DS = Dataset("hubspot_engagements")
