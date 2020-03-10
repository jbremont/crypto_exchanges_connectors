import requests
from exchanges.coinmarketcap.coinmarketcap_pro.enums import expected_api_http_error_code


class Client(object):

    API_URL = 'https://pro-api.coinmarketcap.com'
    API_VERSION = 'v1'

    def __init__(self, api_key):

        self.API_KEY = api_key
        self.session = self.init_session()

    def init_session(self):
        session = requests.session()
        session.headers.update({'Accept' : 'application/json',
                                'Accept-Encoding' : 'deflate, gzip',
                                'X-CMC_PRO_API_KEY' : self.API_KEY})
        return session
    
    def request_api_endpoint(self, endpoint, params = {}, timeout = 10):
        api_uri = self.API_URL + '/' + self.API_VERSION + '/' + endpoint

        response = self.session.get(url = api_uri, params = params, timeout = timeout)

        if not response.ok and int(response.status_code) not in expected_api_http_error_code:
            # unexpected http error
            # return http error code/msg structured same as expected API error response
            return {
                "status" : {
                    "error_code" : int(response.status_code),
                    "error_message" : "HTTP Request ERROR (" + str(response.status_code) + ")"
                }
            }
        
        # parse JSON then return
        return response.json()
