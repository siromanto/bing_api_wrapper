# -*- coding: utf-8 -*-

import datetime
import csv
import re
import time
import pandas as pd

from bing.api import BingWebmasterApi
from config import config, helpers

api = BingWebmasterApi()
url = 'www.snowflake.com'
target_url = None

endpoint_args = {
    'GetQueryStats': {
        'siteUrl': config.BING_SITE_URL
    },
    'GetPageStats': {
        'siteUrl': config.BING_SITE_URL
    },
    'GetPageQueryStats': {
        'siteUrl': config.BING_SITE_URL,
        'url': target_url
    }
}


def prepare_header_for_clear_csv(file, headers):
    writer = csv.DictWriter(file, fieldnames=headers)
    writer.writeheader()
    return writer


def parse_date(string_date):
    a = re.search(r'\d+', string_date)
    timestamp = a.group(0)
    return time.strftime("%Y-%m-%d", time.gmtime(int(timestamp) / 1000.0))


def extrract_weekly():
    pass


# def extract_data(endpoint_name):
#     data = getattr(api, endpoint_name)(**endpoint_args.get(endpoint_name))
#     raw_data = data['d']
#
#     print(data)
#
#     if not raw_data:
#         return 'Response are empty'
#
#     with open(f'../data/{endpoint_name.lower()}.csv', mode='w', encoding='utf8') as raw_csv:
#
#         headers = list(raw_data[0].keys())
#         writer = prepare_header_for_clear_csv(raw_csv, headers)
#
#         for row in raw_data:
#             writer.writerow(row)


"""
    source - 

"""

def extract_data():
    aggregated_data = []
    pages_raw_data = api.GetPageStats(siteUrl=config.BING_SITE_URL)

    # target_pages = [x.get('Query') for x in pages_raw_data.get('d')]
    global_info_pages = {x.get('Query'): x for x in pages_raw_data.get('d')}

    with open(f'../data/{"Get_Page_Query_Stats".lower()}.csv', mode='w', encoding='utf8') as raw_csv:
        writer = prepare_header_for_clear_csv(
            raw_csv, helpers.CSV_COLUMNS)

        for target_page, page_data in global_info_pages.items():
            page_detailed_info = api.GetPageQueryStats(siteUrl=config.BING_SITE_URL, page=target_page).get('d')
            row = {}

            print('*'*200)
            print(f'WORKING WITH {target_page} PAGE')

            row.update({
                'SOURCE': page_data.get('__type').split('#')[1],
                'DATE': None,
                'TARGET_URL': target_page,
                'TOTAL_PAGE_AVG_CLICK_POSITION': page_data.get('AvgClickPosition'),
                'TOTAL_PAGE_AVG_IMPRESSION_POSITION': page_data.get('AvgImpressionPosition'),
                'TOTAL_PAGE_CLICS': page_data.get('Clicks'),
                'TOTAL_PAGE_IMPRESSIONS': page_data.get('Impressions'),
                'QUERY_AVG_CLICK_POSITION': 0,
                'QUERY_AVG_IMPRESSION_POSITION': 0,
                'QUERY_CLICKS': 0,
                'QUERY_IMPRESSIONS': 0,
                'QUERY': None
            })

            # TOTAL CLICS FOR PAGE ------>>>

            if page_detailed_info:
                print('*'*200)
                print(f'WE HAVE ATACHED DATA ---> {len(page_detailed_info)}')
                for detailed_info in page_detailed_info:
                    row.update({
                        'QUERY_AVG_CLICK_POSITION': detailed_info.get('AvgClickPosition'),
                        'QUERY_AVG_IMPRESSION_POSITION': detailed_info.get('AvgImpressionPosition'),
                        'DATE': parse_date(detailed_info.get('Date')),
                        'QUERY_CLICKS': detailed_info.get('Clicks'),
                        'QUERY_IMPRESSIONS': detailed_info.get('Impressions'),
                        'QUERY': detailed_info.get('Query')
                    })

                    print('*' * 200)
                    print(row)
                    print('*' * 200)

                    # aggregated_data.append(row)

                    writer.writerow(row)

                    # print(aggregated_data)
            else:
                row.update(DATE=parse_date(page_data.get('Date')))
                # aggregated_data.append(row)
                writer.writerow(row)

            print('='*200)

        print(aggregated_data)





if __name__ == '__main__':
    extract_data()



    # extract_data('GetQueryStats')
    # extract_data('GetQueryStats')

    # Get list of site pages which has inbound links ||
    # print(api.GetQueryStats(siteUrl=config.BING_SITE_URL, page=0))
    # print('*' * 200)
    # print(getattr(api, 'GetQueryStats')(siteUrl=config.BING_SITE_URL, page=0))
    # print('*' * 200)
    # print(getattr(api, 'GetQueryStats')(**methods_args.get('GetQueryStats')))