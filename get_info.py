import datetime

from bing.api import BingWebmasterApi


api = BingWebmasterApi()


if __name__ == '__main__':
    site = 'https://www.snowflake.com'

    # Get list of site pages which has inbound links ||
    print(api.GetLinkCounts(siteUrl=site, page=0))
    print('*' * 200)

    # Get index traffic details for directory ||
    print(api.GetChildrenUrlTrafficInfo(siteUrl=site, url='www.snowflake.com', page=0))
    print('*' * 200)

    # List of site's impressions and clicks for for the last 6 months || The data will be updated every day.
    print(api.GetRankAndTrafficStats(siteUrl=site))
    print('*' * 200)

    # List of feeds. The list will contain only top-level feeds: individual sitemaps and sitemap indices.
    print(api.GetFeeds(siteUrl=site))
    print('*' * 200)

    # List of keywords which trigger impression of the site on SERP for the specific page. || The data will be updated every week.
    print(api.GetPageQueryStats(siteUrl=site, page='https://www.snowflake.com/product/architecture/'))
    print('*'*200)

    # Get detailed traffic statistics for top pages. || The data will be updated every week.
    print(api.GetPageStats(siteUrl=site))
    print('*'*200)

    # Get index traffic details for single page
    # Remarks: "domain:" prefix can be used to get information for domain. For example: domain:bing.com
    print(api.GetUrlTrafficInfo(siteUrl=site, url='www.snowflake.com'))



