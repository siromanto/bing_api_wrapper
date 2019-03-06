import datetime

from bing.api import BingWebmasterApi
from config import config
import csv
import pandas as pd

api = BingWebmasterApi()
url = 'www.snowflake.com'

endpoint_args = {
    'GetQueryStats': {
        'siteUrl': config.BING_SITE_URL
    },
    'GetChildrenUrlTrafficInfo': {
        'siteUrl': config.BING_SITE_URL,
        'url': url
    }
}


def prepare_header_for_clear_csv(file, headers):
    writer = csv.DictWriter(file, fieldnames=headers)
    writer.writeheader()
    return writer

def extract_data(endpoint_name):
    data = getattr(api, endpoint_name)(**endpoint_args.get(endpoint_name))
    raw_data = data['d']

    print(data)

    if not raw_data:
        return 'Response are empty'

    with open(f'../data/{endpoint_name.lower()}.csv', mode='w', encoding='utf8') as raw_csv:


        headers = list(raw_data[0].keys())
        writer = prepare_header_for_clear_csv(raw_csv, headers)

        for row in raw_data:
            writer.writerow(row)








if __name__ == '__main__':
    extract_data('GetQueryStats')

    # Get list of site pages which has inbound links ||
    # print(api.GetQueryStats(siteUrl=config.BING_SITE_URL, page=0))
    # print('*' * 200)
    # print(getattr(api, 'GetQueryStats')(siteUrl=config.BING_SITE_URL, page=0))
    # print('*' * 200)
    # print(getattr(api, 'GetQueryStats')(**methods_args.get('GetQueryStats')))