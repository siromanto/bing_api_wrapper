# -*- coding: utf-8 -*-

import os
import pandas as pd

from configs import config, helpers


def _execute_queries_for_upload(curr, report_path, storage_path, table_name):
    curr.execute('PUT \'file://{}\' \'{}\''.format(report_path, storage_path))
    curr.execute('COPY INTO {} FROM \'{}\' '
                               'FILE_FORMAT=(SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY=\'"\')'.format(table_name,
                                                                                                       storage_path))
    curr.execute('REMOVE \'{}\''.format(storage_path))


def load_data():
    report_path = config.DATA_PATH
    load_raw_data_from_csv(report_path)


def load_weekly():
    pass


def load_raw_data_from_csv(file_path):
    file_name = file_path.rsplit('/', 1)[-1]

    client_config = helpers.get_client_config(r'/Users/siromanto/ralabs/0.projects/conDati/BingAds/config/BingConsole.json')
    db_config = helpers.get_client_config('../config/Siromanto_account.json')

    # client_config = helpers.get_client_config(r'/opt/workbench/users/afuser/airflow/dags/credentials/BingConsole/Toweltech.json')
    # db_config = helpers.get_client_config('credentials/SnowflakeKeys/CONDATI ----> .json')

    conn = helpers.establish_db_conn(
        db_config['user'],
        db_config['pwd'],
        db_config['account'],
        client_config['raw_db'],  # Change this in prod env
        client_config['warehouse']  # Change this in prod env
    )

    curr = conn.cursor()
    table_name = client_config['raw_table']

    storage_path = '@%{}/{}'.format(table_name, file_name)
    try:
        curr.execute('BEGIN')
        # _cleanup_data(curr, table_name)
        _execute_queries_for_upload(curr, file_path, storage_path, table_name)
        curr.execute('COMMIT')

        print(f'FINISH FILE LOADING...')

    except Exception as e:
        print(e)
    finally:
        conn.cursor().close()
        conn.close()
        print(f"Data imported successfully")
    # os.remove(file_path)


if __name__ == '__main__':
    load_data()
