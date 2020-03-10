import requests
from pandas import to_datetime


class CryptoCompare(object):
    __coinlist_url = 'https://www.cryptocompare.com/api/data/coinlist/'
    __exchlist_url = 'https://min-api.cryptocompare.com/data/all/exchanges'
    __sintargetprice_url = 'https://min-api.cryptocompare.com/data/price?fsym={}&tsyms={}'
    __multiprice_url = 'https://min-api.cryptocompare.com/data/pricemulti?fsyms={}&tsyms={}'
    __multipricefull_url = 'https://min-api.cryptocompare.com/data/pricemultifull?fsyms={}&tsyms={}'

    @staticmethod
    def api_query(base_symbols, comparison_symbols, exchange='', full_market=False):
        '''
        base_symbol: list or string
        comparison_symbols: list or string
        '''

        if type(comparison_symbols) is str:
            conv = comparison_symbols.upper()
        elif type(comparison_symbols) is list:
            conv = ','.join(comparison_symbols).upper()

        if full_market:
            if type(base_symbols) is str:
                conv1 = base_symbols.upper()
            elif type(base_symbols) is list:
                conv1 = ','.join(base_symbols).upper()
            url = CryptoCompare.__multipricefull_url \
                .format(conv1, conv)
        else:
            if type(base_symbols) is str:
                conv1 = base_symbols.upper()
                url = CryptoCompare.__sintargetprice_url \
                    .format(conv1, conv)
            elif type(base_symbols) is list:
                conv1 = ','.join(base_symbols).upper()
                url = CryptoCompare.__multiprice_url \
                    .format(conv1, conv)

        # if no exchange is specified, cryptocompare aggregated average (CCCAGG) is used
        if exchange:
            url = url + '&e={}'.format(exchange)

        page = requests.get(url)
        data = page.json()
        return data

    @staticmethod
    def coin_list_info():
        page = requests.get(CryptoCompare.__coinlist_url)
        data = page.json()['Data']
        return data

    @staticmethod
    def exchange_list():
        page = requests.get(CryptoCompare.__exchlist_url)
        data = page.json().keys()
        return list(data)

    @staticmethod
    def pairs_available(exchange):
        page = requests.get(CryptoCompare.__exchlist_url)
        data = page.json()
        lst = []
        x = data[exchange]
        for keys in x:
            for elements in x[keys]:
                lst.append(keys + '/' + elements)
        return lst

    @staticmethod
    def raw_price(base_symbols, comparison_symbols, exchange=''):
        '''
        base_symbol: list or string
        comparison_symbols: list or string
        return a dictionary with keys as currency pair symbols and values as conversion rate
        '''

        data = CryptoCompare.api_query(base_symbols, comparison_symbols, exchange)
        result = {}

        if type(base_symbols) is str:
            for keys in data.keys():
                result[base_symbols.upper() + '/' + keys] = data[keys]
        elif type(base_symbols) is list:
            base_index = [base.upper() for base in base_symbols]
            if type(comparison_symbols) is list:
                comparison_index = [comp.upper() for comp in comparison_symbols]
                for symbols in base_index:
                    for symbs in comparison_index:
                        result[symbols + '/' + symbs] = data[symbols][symbs]
            elif type(comparison_symbols) is str:
                for symbols in base_index:
                    result[symbols + '/' + comparison_symbols.upper()] = data[symbols.upper()][
                        comparison_symbols.upper()]
        return result

    @staticmethod
    def curncy_conversion(from_curncy, to_curncy, from_c_qty, exchange=''):
        '''
        from_curncy: string - currency to convert from
        from_c_qty: float - quantity of the currency to convert from
        to_curncy: string or list - currencies to convert to
        exchange: string - specified exchange

        return: float or a list of float
        '''

        conversion_rate = CryptoCompare.raw_price(base_symbols=from_curncy, comparison_symbols=to_curncy,
                                                  exchange=exchange)
        from_curncy = from_curncy.upper()
        if type(to_curncy) is str:
            to_curncy = to_curncy.upper()
            pairs = from_curncy + '/' + to_curncy
            to_curncy_values = from_c_qty * conversion_rate[pairs]

        elif type(to_curncy) is list:
            to_curncy = ','.join(to_curncy).upper()
            pairs = []
            for assets in to_curncy:
                pairs.append(from_curncy + '/' + assets)

            to_curncy_values = []
            for keys in conversion_rate:
                to_curncy_values.append(from_c_qty * conversion_rate[keys])

        return to_curncy_values

    @staticmethod
    def price_poller(bases, quotes, exchange=''):
        data = (CryptoCompare.api_query(bases, quotes, exchange=exchange, full_market=True))['RAW']
        result = []
        columns = dict.fromkeys(['timestamp', 'base', 'quote', 'price'])

        if type(bases) is str:
            base_index = [bases.upper()]
        elif type(bases) is list:
            base_index = [base.upper() for base in bases]

        if type(quotes) is str:
            quote_index = [quotes.upper()]
        elif type(quotes) is list:
            quote_index = [quote.upper() for quote in quotes]

        for i in base_index:
            for j in quote_index:
                columns['timestamp'] = to_datetime(data[i][j]['LASTUPDATE'], unit='s', utc=True).to_pydatetime()
                columns['base'] = i
                columns['quote'] = j
                columns['price'] = data[i][j]['PRICE']
                result.append(columns.copy())
        return result
