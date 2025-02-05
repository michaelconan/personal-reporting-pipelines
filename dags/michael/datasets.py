"""
datasets.py

Common definitions of Airflow datasets for triggering DAGs.
"""

from airflow.datasets import Dataset

# Datasets for triggering
NOTION_DAILY_HABITS_DS = Dataset("notion_daily_habits")
NOTION_WEEKLY_HABITS_DS = Dataset("notion_weekly_habits")
