""" Pyhton test API client for Bing Webmaster Tools

Methods: https://msdn.microsoft.com/en-us/library/jj572365.aspx
"""

import httplib2
import json
from urllib.parse import urlencode

from config import config


class BingWebmasterApi:

    def __init__(self, api_key=None, timeout=3):
        self.api_key = api_key or config.API_KEY
        self.endpoint = 'https://www.bing.com/webmaster/api.svc/json/'
        self.h = httplib2.Http("/tmp/.cache", timeout=timeout)

    def __getattr__(self, item):
        def call(**kwargs):
            _uri = '{endpoint}{function}?{query}'.format(
                endpoint=self.endpoint,
                function=item,
                query=urlencode(kwargs)
            )
            resp, content = self.h.request(_uri, 'GET')

            if not resp.status == 200:
                print(content)  #Stub for errors
            else:
                return json.loads(content)

        return call