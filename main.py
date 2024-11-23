import argparse
import sys
from typing import Final, Tuple

import numpy as np
import pandas as pd

from src.bigquery_manager import BigQuery
from src.common import logger

parser = argparse.ArgumentParser()

parser.add_argument('-d', '--delete', default=False)
args = parser.parse_args()

if __name__ == '__main__':
    bq: BigQuery = BigQuery()

    if args.delete:
        bq.delete_dataset()
        sys.exit()

    np.random.seed(1234)

    SIZE: Final[int] = 10000
    USER_TYPE: Tuple[str, ...] = ('新規', '既存')
    CATEGORIES: Tuple[str, ...] = ('pc', 'phone', 'tablet')

    order_date: np.ndarray = pd.date_range('2023-01-01', '2024-11-01').values
    user_id: np.ndarray = np.random.randint(1, 100000, size=SIZE)
    sales: np.ndarray = np.random.randint(4000, 500000, size=SIZE)

    user_type: np.ndarray = np.random.choice(USER_TYPE, SIZE)
    category: np.ndarray = np.random.choice(CATEGORIES, SIZE)

    logger.info('Create sample data: %d' % SIZE)

    df = pd.DataFrame({
        'id': np.arange(0, SIZE),
        'order_date': np.random.choice(order_date, SIZE),
        'user_id': user_id,
        'sales': sales,
        'user_type': user_type,
        'category': category,
        'day': np.random.choice([1, 2, 3, 4, 5], SIZE).astype(int),
        'set': np.random.choice([100, 200, 300, 400, 500, 600, 700, 800, 900, 1000], SIZE).astype(int)
    })

    bq.create_dataset()
    bq.create_table()
    bq.load_table_from_dataframe(df)
