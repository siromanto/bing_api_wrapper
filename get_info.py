from bing.api import BingWebmasterApi

api = BingWebmasterApi()


if __name__ == '__main__':
    print(api.GetLinkCounts(siteUrl='https://www.snowflake.com', page=0))

