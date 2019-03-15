import json, os
from datetime import timedelta, datetime
import re
import time
import csv
import snowflake.connector as connector

from . import config


# DATETIME_FORMAT = '%m/%d/%Y'

CSV_COLUMNS = ['SOURCE', 'DATE', 'TARGET_URL', 'TOTAL_PAGE_AVG_CLICK_POSITION', 'TOTAL_PAGE_AVG_IMPRESSION_POSITION',
               'TOTAL_PAGE_CLICS', 'TOTAL_PAGE_IMPRESSIONS', 'QUERY_AVG_CLICK_POSITION',
               'QUERY_AVG_IMPRESSION_POSITION', 'QUERY_CLICKS', 'QUERY_IMPRESSIONS', 'QUERY']


# def normalize_backfill_start_end_time(start_date, end_date):
#     end_time = (end_date + timedelta(days=1) - timedelta(seconds=1)).strftime(DATETIME_FORMAT)
#     start_time = start_date.strftime(DATETIME_FORMAT)
#     return start_time, end_time


def establish_db_conn(user, password, account, db, warehouse):
    conn = connector.connect(
        user=user,
        password=password,
        account=account
    )
    conn.cursor().execute('USE DATABASE {}'.format(db))
    conn.cursor().execute('USE WAREHOUSE {}'.format(warehouse))
    return conn


def get_client_config(conf_path, client_name=None):
    if client_name is None:
        with open(conf_path, 'r') as f:
            conf = json.load(f)
    else:
        with open(conf_path, 'r') as f:
            conf = json.load(f).get(client_name)
    assert conf is not None
    return conf


def perform_db_routines(client_name, sql):
    # configfile = get_resource_path()[0]

    # client_config = get_client_config(client_name, configfile)
    conn = establish_db_conn(config.SNOWFLAKE_DB_USERNAME,
                             config.SNOWFLAKE_DB_PASSWORD,
                             config.SNOWFLAKE_DB_ACCOUNT,
                             config.SNOWFLAKE_DATABASE,
                             config.SNOWFLAKE_WAREHOUSE)
    conn.autocommit(False)
    curr = conn.cursor()
    queries_list = sql.split(';')
    try:
        curr.execute('BEGIN')
        for q in queries_list:
            curr.execute(q)
        curr.execute('COMMIT')
    finally:
        curr.close()
        conn.close()


def print_header(name):
    print('*' * 200)
    print(f'PREPARE FILES TO LOAD --- {name.upper()}')
    print('*' * 200)


def get_data_by_chunks(items_list, n):
    for i in range(0, len(items_list), n):
        yield items_list[i:i + n]


def parse_date(string_date):
    a = re.search(r'\d+', string_date)
    timestamp = a.group(0)
    return time.strftime("%Y-%m-%d", time.gmtime(int(timestamp) / 1000.0))


def prepare_header_for_clear_csv(file, headers):
    writer = csv.DictWriter(file, fieldnames=headers)
    writer.writeheader()
    return writer
