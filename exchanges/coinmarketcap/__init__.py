from exchanges.coinmarketcap.coinmarketcap_pro.client import Client

class CoinMarketCapService(object):
    def __init__(self, api_key):
        self.client = Client(api_key)

    def get_market_pairs(self, symbol):
        return self.client.request_api_endpoint('cryptocurrency/market-pairs/latest', params={'symbol': symbol, 'convert': 'USD'})

if __name__ == '__main__':
    cmcs = CoinMarketCapService('REDACTED')
    result = cmcs.get_market_pairs('ARK')
    print(result)
