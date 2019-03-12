# -*- coding: utf-8 -*-

import os
import pandas as pd

from config import config, helpers


class Load:

    def __init__(self, con_config):
        self.config = con_config
        self.conn = self.create_connection()

    def create_connection(self):
        conn = helpers.establish_db_conn(
            self.config.SNOWFLAKE_DB_USERNAME,
            self.config.SNOWFLAKE_DB_PASSWORD,
            self.config.SNOWFLAKE_DB_ACCOUNT,
            self.config.SNOWFLAKE_DATABASE,
            self.config.SNOWFLAKE_WAREHOUSE
        )
        return conn

    def load_raw_data_from_csv(self, table_name):

        for file in os.listdir('/Users/siromanto/edu/python/BingAds/data/'):
            if file.endswith('.csv'):
                print(f'Try to load file: {file} ===>')

                file_path = f"/Users/siromanto/edu/python/BingAds/data/{file}"

                curr = self.conn.cursor()
                storage_path = '@%{}/{}'.format(table_name, file)

                try:
                    curr.execute('BEGIN')
                    # self._cleanup_data(curr, table_name)
                    self._execute_queries_for_upload(file_path, storage_path, table_name)
                    curr.execute('COMMIT')

                    print(f'FINISH FILE LOADING...')
                except Exception as e:
                    print(e)

        self.conn.cursor().close()
        self.conn.close()
        print(f"Data imported successfully")

    def _execute_queries_for_upload(self, report_path, storage_path, table_name):
        self.conn.cursor().execute('PUT \'file://{}\' \'{}\''.format(report_path, storage_path))
        self.conn.cursor().execute('COPY INTO {} FROM \'{}\' '
                                   'FILE_FORMAT=(SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY=\'"\')'.format(table_name, storage_path))
        self.conn.cursor().execute('REMOVE \'{}\''.format(storage_path))

    def load(self, table_name=None):
        self.load_raw_data_from_csv(table_name)


if __name__ == '__main__':
    Load(config).load(table_name='BING_WEBMASTER_QUERY_STATS')
