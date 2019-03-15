#!/usr/bin/env python
import snowflake.connector

import config


def create_connector():
    # Gets the version
    ctx = snowflake.connector.connect(
        user=config.SNOWFLAKE_DB_USERNAME,
        password=config.SNOWFLAKE_DB_PASSWORD,
        account=config.SNOWFLAKE_DB_ACCOUNT
        )
    return ctx


def run():
    ctx = create_connector()
    cs = ctx.cursor()

    try:
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        print(one_row[0])

        cs.execute('USE WAREHOUSE {}'.format(config.SNOWFLAKE_WAREHOUSE))
        cs.execute('USE DATABASE {}'.format(config.SNOWFLAKE_DATABASE))

        cs.execute(
            "CREATE OR REPLACE TABLE "
            "BING_WEBMASTER_QUERY_STATS({})".format(config.QUERY_STATS_DB_COLUMNS))

        print(f'Database BING_WEBMASTER_QUERY_STATS successfully created or cleared')
    finally:
        cs.close()
    ctx.close()


if __name__ == '__main__':
    run()
