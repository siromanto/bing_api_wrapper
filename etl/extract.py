# -*- coding: utf-8 -*-

from bing.api import BingWebmasterApi
from config import config, helpers


def extract_weekly():
    pass


def extract_data():
    api = BingWebmasterApi()
    pages_raw_data = api.GetPageStats(siteUrl=config.BING_SITE_URL)

    global_info_pages = {x.get('Query'): x for x in pages_raw_data.get('d')}

    with open(f'../data/{"Get_Page_Query_Stats".lower()}.csv', mode='w', encoding='utf8') as raw_csv:
        writer = helpers.prepare_header_for_clear_csv(raw_csv, helpers.CSV_COLUMNS)

        for target_page, page_data in global_info_pages.items():
            page_detailed_info = api.GetPageQueryStats(siteUrl=config.BING_SITE_URL, page=target_page).get('d')
            row = {}

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

            if page_detailed_info:
                # print(f'WE HAVE ATACHED DATA ---> {len(page_detailed_info)}')
                for detailed_info in page_detailed_info:
                    row.update({
                        'QUERY_AVG_CLICK_POSITION': detailed_info.get('AvgClickPosition'),
                        'QUERY_AVG_IMPRESSION_POSITION': detailed_info.get('AvgImpressionPosition'),
                        'DATE': detailed_info.get('Date'),
                        'QUERY_CLICKS': detailed_info.get('Clicks'),
                        'QUERY_IMPRESSIONS': detailed_info.get('Impressions'),
                        'QUERY': detailed_info.get('Query')
                    })

                    writer.writerow(row)
            else:
                row.update(DATE=page_data.get('Date'))
                writer.writerow(row)


if __name__ == '__main__':
    extract_data()
