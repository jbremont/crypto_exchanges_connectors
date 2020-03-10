import http.client
import urllib
import json
import hashlib



def build_signature(params, secret_key):
    sign = ''
    for key in sorted(params.keys()):
        sign = sign + key + '=' + str(params[key]) +'&'
    data = sign + 'secret_key=' + secret_key
    return hashlib.md5(data.encode('utf-8')).hexdigest().upper()

def http_get(url, resource, params=''):
    conn = http.client.HTTPSConnection(url, timeout=10)
    conn.request("GET",resource + '?' + params)
    response = conn.getresponse()
    data = response.read().decode('utf-8')
    return json.loads(data)

def http_post(url,resource,params):
    headers = {
    "Content-type" : "application/x-www-form-urlencoded",
    }
    conn = http.client.HTTPSConnection(url, timeout=10)
    temp_params = urllib.parse.urlencode(params)
    conn.request("POST", resource, temp_params, headers)
    response = conn.getresponse()
    data = response.read().decode('utf-8')
    params.clear()
    conn.close()
    return json.loads(data)

class RestClient(object):

    __url = 'www.okex.com'
    def __init__(self, api_key, secret_key):

        self.__api_key = api_key
        self.__secret_key = secret_key

    def market_depth(self, symbol=''):
        DEPTH_RESOURCE = '/api/v1/depth.do'
        params = ''
        if symbol:
            params = 'symbol=%(symbol)s' % {'symbol': symbol}

        return http_get(self.__url, DEPTH_RESOURCE, params)

    def trade_history(self, symbol=''):
        TRADES_RESOURCE = '/api/v1/trades.do'
        params = ''
        if symbol:
            params = 'symbol=%(symbol)s' % {'symbol': symbol}

        return http_get(self.__url, TRADES_RESOURCE, params)

    def user_info(self):
        '''
        :return: Funds available in spot trading account
        '''
        USERINFO_RESOURCE = '/api/v1/userinfo.do'
        params = {}
        params['api_key'] = self.__api_key
        params['sign'] = build_signature(params, self.__secret_key)

        return http_post(self.__url, USERINFO_RESOURCE, params)

    def wallet_info(self):
        '''
        :return: Total funds available in wallet
        '''
        WALLETINFO_RESOURCE = '/api/v1/wallet_info.do'
        params = {}
        params['api_key'] = self.__api_key
        params['sign'] = build_signature(params, self.__secret_key)

        return http_post(self.__url, WALLETINFO_RESOURCE, params)

    def place_order(self, symbol, order_type, price='', amount=''):
        TRADE_RESOURCE = '/api/v1/trade.do'
        params = {
            'api_key': self.__api_key,
            'symbol': symbol,
            'type': order_type   # limit order(buy/sell) market order(buy_market/sell_market)
        }
        if price:
            params['price'] = price        # For limit orders, the price must be between 0~1,000,000.
                                                    # IMPORTANT: for market buy orders, the price is to total
                                                    # amount you want to buy, and it must be higher than the current price
                                                    # of 0.01 BTC (minimum buying unit), 0.1 LTC or 0.01 ETH.
                                                    # For market sell orders, the price is not required
        if amount:
            params['amount'] = amount        # Must be higher than 0.01 for BTC, 0.1 for LTC or 0.01 for ETH.
                                                    # For market buy roders, the amount is not required

        params['sign'] = build_signature(params, self.__secret_key)


        return http_post(self.__url, TRADE_RESOURCE, params)

    def place_limit_order(self, symbol, side, price, amount):
        return self.place_order(symbol=symbol, order_type=side, price=price, amount=amount)

    def place_market_order(self, symbol, side, amount):
        order_type = side + '_' + 'market'
        return self.place_order(symbol=symbol, order_type=order_type, price=amount)


    def place_batch_orders(self, symbol, order_type, orders_data):
        BATCH_TRADE_RESOURCE = '/api/v1/batch_trade.do'
        params = {
            'api_key': self.__api_key,
            'symbol': symbol,
            'type': order_type,
            'orders_data': orders_data              # JSON string Example: [{price:3,amount:5,type:'sell'},
                                                    # {price:3,amount:3,type:'buy'},{price:3,amount:3}]
                                                    # max order number is 5，for 'price' and 'amount' parameter,
                                                    # refer to trade/API. Final order type is decided primarily by
                                                    # 'type' field within 'orders_data' and subsequently by 'type' field
                                                    # (if no 'type' is provided within 'orders_data' field)
        }
        params['sign'] = build_signature(params, self.__secret_key)

        return http_post(self.__url, BATCH_TRADE_RESOURCE, params)

    def cancel_order(self, symbol, order_id):
        CANCEL_ORDER_RESOURCE = '/api/v1/cancel_order.do'
        params = {
             'api_key': self.__api_key,
             'symbol': symbol,
             'order_id': order_id              # order ID (multiple orders are separated by a comma ',',
                                                    # Max of 3 orders are allowed per request)
        }
        params['sign'] = build_signature(params,self.__secret_key)
        return http_post(self.__url, CANCEL_ORDER_RESOURCE, params)

    def get_order_info_byid(self, symbol, order_id, batch = False, fill_type=''):
        if batch == False:
            ORDER_INFO_RESOURCE = '/api/v1/order_info.do'
        else:
            ORDER_INFO_RESOURCE = '/api/v1/orders_info.do'

        params = {
            'api_key': self.__api_key,
            'symbol': symbol,
            'order_id': order_id
        }
        if fill_type:
            params['type']: fill_type   # 0: unfilled, 1: filled

        params['sign'] = build_signature(params, self.__secret_key)


        return http_post(self.__url, ORDER_INFO_RESOURCE, params)

    def get_orders_info_bysymbol(self, symbol, status, current_page=1, page_length=200):
        # only the most recent two days are returned
        ORDER_HISTORY_RESOURCE = '/api/v1/order_history.do'
        params = {
            'api_key': self.__api_key,
            'symbol': symbol,
            'status': status,                         # returns : status: -1 = cancelled, 0 = unfilled, 1 = partially filled,
                                                      # 2 = fully filled, 4 = cancel request in process
            'current_page': current_page,           # current page number
            'page_length': page_length       # number of orders returned per page, maximum 200
        }
        params['sign'] = build_signature(params, self.__secret_key)


        return http_post(self.__url, ORDER_HISTORY_RESOURCE, params)

    def withdraw(self, symbol, trade_pwd, withdraw_address, withdraw_amount, address_type='address'):
        WITHDRAW_RESOURCE = '/api/v1/withdraw.do'
        params = {
            'api_key': self.__api_key,
            'symbol': symbol,
            # 'chargefee': tran_fee,
            'trade_pwd': trade_pwd,
            'withdraw_address': withdraw_address,
            'withdraw_amount': withdraw_amount,
            'target': address_type   # withdraw address type. okcoin.cn:"okcn" okcoin.com:"okcom"
                                     # okes.com："okex_service" outer address:"address"
        }
        params['sign'] = build_signature(params, self.__secret_key)

        return http_post(self.__url, WITHDRAW_RESOURCE, params)

    def cancel_withdraw(self, symbol, withdraw_id):
        CANCEL_WITHDRAW_RESOURCE = '/api/v1/withdraw_info.do'
        params = {
            'api_key': self.__api_key,
            'symbol': symbol,
            'withdraw_id': str(withdraw_id)

        }
        params['sign'] = build_signature(params, self.__secret_key)

        return http_post(self.__url, CANCEL_WITHDRAW_RESOURCE, params)

    def withdraw_info(self, symbol, withdraw_id):
        WITHDRAWINFO_RESOURCE = '/api/v1/withdraw_info.do'
        params = {
            'api_key': self.__api_key,
            'symbol': symbol,
            'withdraw_id': str(withdraw_id)
        }
        params['sign'] = build_signature(params, self.__secret_key)

        return http_post(self.__url, WITHDRAWINFO_RESOURCE, params)

    def deposit_withdraw_record(self, symbol, dw_type, current_page, page_length):
        DW_RESOURCE = '/api/v1/account_records.do'
        params = {
            'api_key': self.__api_key,
            'symbol': symbol,             # only xxx_usd supported
            'type': dw_type, # 0: deposits, 1: withdraws
            'current_page': current_page,
            'page_length': page_length
        }
        params['sign'] = build_signature(params, self.__secret_key)

        return http_post(self.__url, DW_RESOURCE, params)

    def internal_fund_transfer(self, symbol, amount, from_acc, to_acc):
        IFTRANSFER_RESOURCE = '/api/v1/funds_transfer.do'
        params = {
            'api_key': self.__api_key,
            'symbol': symbol,
            'amount': amount,
            'from': from_acc,
            'to': to_acc
        }
        params['sign'] = build_signature(params, self.__secret_key)

        return http_post(self.__url, IFTRANSFER_RESOURCE, params)

    def tickers_market_info(self, symbol=''):
        TICKER_RESOURCE = '/api/v1/tickers.do'
        params = ''
        if symbol:
            params = 'symbol=%(symbol)s' % {'symbol': symbol}

        return http_get(self.__url, TICKER_RESOURCE, params)

    def ticker_list(self):
        resp = self.tickers_market_info()
        result = []
        for elements in resp['tickers']:
            result.append(elements['symbol'])

        return result