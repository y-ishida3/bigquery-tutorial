from logging import getLogger
from typing import Final, Tuple

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from .common import logging, logger


class BigQuery:

    def __init__(self, project: str = 'sandbox-430005', location: str = 'asia-northeast1') -> None:
        self._client: Final[bigquery.Client] = bigquery.Client(
            project=project,
            location=location
        )

    @property
    def dataset(self) -> str:
        return f'{self._client.project}.sample'

    @property
    def table(self) -> str:
        return f'{self.dataset}.purchase_history'

    @property
    def schema(self) -> Tuple[bigquery.SchemaField, ...]:
        return (
            bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('order_date', 'DATE', mode='REQUIRED'),
            bigquery.SchemaField('category', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('user_type', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('user_id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('sales', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('day', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('set', 'INTEGER', mode='REQUIRED'),
        )

    @logging
    def create_dataset(self) -> None:
        if not self.is_exists_dataset():
            self._client.create_dataset(bigquery.Dataset(self.dataset), timeout=30)

    @logging
    def delete_dataset(self):
        if self.is_exists_dataset():
            self._client.delete_dataset(
                bigquery.Dataset(self.dataset),
                timeout=30,
                delete_contents=True,
                not_found_ok=True
            )

    @logging
    def create_table(self):
        if not self.is_exists_table():
            self._client.create_table(bigquery.Table(self.table, schema=self.schema))

    @logging
    def delete_table(self):
        if self.is_exists_table():
            self._client.delete_table(bigquery.Table(self.table, schema=self.schema))

    @logging
    def load_table_from_dataframe(self, dataframe):
        self._client.load_table_from_dataframe(dataframe, self.table)

    def is_exists_dataset(self) -> bool:
        try:
            self._client.get_dataset(self.dataset)
            logger.info(f'{self.dataset} is exists')
            return True
        except NotFound:
            return False

    def is_exists_table(self) -> bool:
        try:
            self._client.get_table(self.table)
            logger.info(f'{self.table} is exists')
            return True
        except NotFound:
            return False
