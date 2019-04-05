# -*- coding: utf-8 -*-

from bing.api import BingWebmasterApi
from configs import config, helpers
# from BingSearchConsole.bing.api import BingWebmasterApi
# from BingSearchConsole.configs import config, helpers


def extract_data(**kwargs):
    credentials = helpers.get_client_config(conf_path=config.CLIENT_CONFIG_PATH)
    api = BingWebmasterApi(api_key=credentials["api_key"])

    pages_raw_data = api.GetPageStats(siteUrl=credentials["bing_site_url"])
    global_info_pages = {x.get('Query'): x for x in pages_raw_data.get('d')}

    with open(config.DATA_PATH, mode='w', encoding='utf8') as raw_csv:
        writer = helpers.prepare_header_for_clear_csv(raw_csv, helpers.CSV_COLUMNS)
        print('find pages --- {}'.format(len(global_info_pages)))

        for target_page, page_data in global_info_pages.items():
            row = {}
            page_detailed_info = api.GetPageQueryStats(siteUrl=credentials["bing_site_url"], page=target_page).get('d')
            print('Start working with url --- {}...'.format(target_page))

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

            print('Url has atached queries ---> {}'.format(len(page_detailed_info)))
            if page_detailed_info:
                for detailed_info in page_detailed_info:
                    row.update({
                        'QUERY_AVG_CLICK_POSITION': detailed_info.get('AvgClickPosition'),
                        'QUERY_AVG_IMPRESSION_POSITION': detailed_info.get('AvgImpressionPosition'),
                        'DATE': helpers.parse_date(detailed_info.get('Date')),
                        'QUERY_CLICKS': detailed_info.get('Clicks'),
                        'QUERY_IMPRESSIONS': detailed_info.get('Impressions'),
                        'QUERY': detailed_info.get('Query')
                    })
                    writer.writerow(row)
            else:
                row.update(DATE=helpers.parse_date(page_data.get('Date')))
                writer.writerow(row)


if __name__ == '__main__':
    extract_data()
