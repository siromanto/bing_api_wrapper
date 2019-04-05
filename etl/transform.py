# -*- coding: utf-8 -*-

import datetime

from configs import config, helpers
# from BingSearchConsole.configs import config, helpers


def transform_all(**kwargs):
    transform_raw_data()


def transform_weekly(**kwargs):
    today = datetime.date.today()
    idx = (today.weekday() + 1) % 7
    fri = today - datetime.timedelta(7 + idx - 5)
    last_friday = fri.strftime('%Y-%m-%d')

    transform_raw_data(last_friday)


def transform_raw_data(last_friday='2018-01-01'):
    client_config = helpers.get_client_config(config.CLIENT_CONFIG_PATH)
    db_config = helpers.get_client_config(config.DB_CONFIG_PATH)

    # with open('BingSearchConsole/etl/sql/transform_raw_data.sql') as f:
    with open(r'/Users/siromanto/ralabs/0.projects/conDati/BingSearchConsole/etl/sql/transform_raw_data.sql') as f:
        transform_sql = f.read()
        transform_sql = transform_sql.format(
            traffic_by_day_table=client_config['traffic_by_day'],
            db_table_raw=client_config['raw_db_table'],
            prod_table_traffic_by_day=client_config['prod_table_traffic_by_day'],
            start_day=last_friday
        )
    helpers.perform_db_routines(transform_sql, client_config, db_config)

    print('WEEKLY DATA SUCCESSFULLY LOAD...')


if __name__ == '__main__':
    # transform_all()
    transform_weekly()
