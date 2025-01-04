import json

from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException


class NotionHook(BaseHook):

    def __init__(self, conn_id, *args, **kwargs):
        from notion_client import Client

        connection = BaseHook.get_connection(conn_id)
        password = connection.password
        self.client = Client(auth=password)

    