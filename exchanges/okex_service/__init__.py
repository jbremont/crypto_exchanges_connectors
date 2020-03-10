from _decimal import Decimal
from exchanges.exchange import Exchange
from exchanges.okex_service.order_book_socket import OrderBookSocket
from exchanges.okex_service.rest_client import RestClient
from time import sleep, time
from aj_sns.creds_retriever import get_creds
import hashlib
from pandas import to_datetime

try:
    import thread
except ImportError:
    import _thread as thread


class OkexService(Exchange):

    def __init__(self, name, public_key, private_key):
        Exchange.__init__(self, name)
        self.order_book_socket = OrderBookSocket()
        self.markets_following = {}
        self.rest_client = RestClient(public_key, private_key)
        self.open_orders = []

    def get_our_orders_by_decimal_price(self):
        our_orders_by_price = {'bids': {}, 'asks': {}}

        open_orders_copy = self.open_orders.copy()

        for order in open_orders_copy:
            order['price'] = Decimal(str(order['price']))
            order['quantity'] = Decimal(str(order['quantity']))
            order['cum_quantity_filled'] = Decimal(str(order['cum_quantity_filled']))
            if order['side'] == 'buy':
                if str(order['price']) not in our_orders_by_price['bids']:
                    our_orders_by_price['bids'][order['price']] = order['quantity'] - order['cum_quantity_filled']
                else:
                    our_orders_by_price['bids'][order['price']] += order['quantity'] - order['cum_quantity_filled']
            elif order['side'] == 'sell':
                if str(order['price']) not in our_orders_by_price['bids']:
                    our_orders_by_price['asks'][order['price']] = order['quantity'] - order['cum_quantity_filled']
                else:
                    our_orders_by_price['asks'][order['price']] += order['quantity'] - order['cum_quantity_filled']

        return our_orders_by_price

    def get_order_book(self, base, quote):
        resp = self.rest_client.market_depth(OkexService._to_market(base, quote))
        if 'error_code' not in resp.keys():
            internal_book = resp

            self.notify_callbacks('order_book', data=internal_book)

            return internal_book

    def create_order(self, base, quote, price, quantity, side, internal_order_id, order_type='limit', request_id=None,
                         requester_id=None, **kwargs):

        if side == 'buy' or side == 'sell':
            response = self.rest_client.place_limit_order(OkexService._to_market(base, quote), side=side, price=price, amount=quantity)
        else:
            raise NotImplementedError('Side of {} is unknown for {}', side, self.name)

        quantity = str(quantity)
        price = str(price)

        if response is None:
            internal_response = {
                'action': 'CREATE_FAILED',
                'reason': 'UNKNOWN',
                'exchange': self.name,
                'base': base,
                'quote': quote,
                'internal_order_id': internal_order_id,
                'side': side,
                'quantity': quantity,
                'price': price,
                'cum_quantity_filled': 0,
                'received_ms': time() * 1000
            }

        elif 'error_code' in response:
            reason = 'UNKNOWN'
            if response['error_code'] == 1002:    # OKEX API error code
                                                  # 'https://github.com/okcoin-okex/API-docs-OKEx.com/blob/master/API-For-Spot-EN/Error%20Code%20For%20Spot.md'
                reason = 'INSUFFICIENT_FUNDS'
            internal_response = {
                'action': 'CREATE_FAILED',
                'reason': reason,
                'exchange': self.name,
                'base': base,
                'quote': quote,
                'internal_order_id': internal_order_id,
                'side': side,
                'quantity': quantity,
                'price': price,
                'cum_quantity_filled': 0,
                'received_ms': time() * 1000
            }
        elif response['result'] is True:
            exchange_id = response['order_id']
            internal_response = {
                'action': 'CREATED',
                'exchange': self.name,
                'base': base,
                'quote': quote,
                'exchange_order_id': exchange_id,
                'internal_order_id': internal_order_id,
                'side': side,
                'quantity': quantity,
                'price': price,
                'cum_quantity_filled': 0,
                'order_status': 'OPEN',
                'server_ms': time() * 1000,
                'received_ms': time() * 1000
            }

            open_order = internal_response.copy()
            open_order['price'] = Decimal(open_order['price'])
            open_order['quantity'] = Decimal(open_order['quantity'])
            self.open_orders.append(internal_response)

        self.notify_callbacks('trade_lifecycle', data=internal_response)

        return internal_response

    def cancel_order(self, base, quote, internal_order_id, request_id, requester_id=None, exchange_order_id=None):
        index_to_pop = None
        if exchange_order_id is None:
            i = 0
            for open_order in self.open_orders:
                if open_order['internal_order_id'] == internal_order_id:
                    exchange_order_id = open_order['exchange_order_id']
                    index_to_pop = i
                    break
                i += 1

        response = self.rest_client.cancel_order(symbol=OkexService._to_market(base,quote), order_id=exchange_order_id)

        if response['result'] is True:
            i = 0

            if index_to_pop is None:
                for order in self.open_orders:
                    if order['exchange_order_id'] == exchange_order_id:
                        index_to_pop = i
                        break
                    i += 1

            if index_to_pop is not None:
                self.open_orders.pop(index_to_pop)

            internal_response = {
                'action': 'CANCELED',
                'exchange': self.name,
                'base': base,
                'quote': quote,
                'exchange_order_id': exchange_order_id,
                'internal_order_id': internal_order_id,
                'order_status': 'CANCELED',
                'server_ms': int(round(time() * 1000)),
                'received_ms': int(round(time() * 1000))
            }

            self.notify_callbacks('trade_lifecycle', data=internal_response)
        else:
            internal_response = {
                'action': 'CANCEL_FAILED',
                'reason': 'order_not_found',
                'exchange': self.name,
                'base': base,
                'quote': quote,
                'exchange_order_id': exchange_order_id,
                'internal_order_id': internal_order_id,
                'order_status': 'UNKNOWN',
                'server_ms': int(round(time() * 1000)),
                'received_ms': int(round(time() * 1000))
            }
            self.notify_callbacks('trade_lifecycle', data=internal_response)

        # return internal_response

    def cancel_all(self, base, quote):
        '''
        Maximum of 200 unfilled orders
        :param base:
        :param quote:
        :return:
        '''
        open_orders_resp = self.rest_client.get_orders_info_bysymbol(symbol=OkexService._to_market(base, quote), status=0)
        open_orders = open_orders_resp['orders']
        for open_order in open_orders:

            exchange_order_id = open_order['orders_id']
            internal_order_id = None

            for order in self.open_orders:
                if order['exchange_order_id'] == exchange_order_id:
                    internal_order_id = order['internal_order_id']
                    break

            self.cancel_order(base, quote, internal_order_id, 'a_request_id', exchange_order_id=exchange_order_id)

    def unfollow_market(self, base, quote):
        self.markets_following.pop(self._to_market(base, quote), None)
        self.order_book_socket.remove_subscription(OkexService._to_market(base, quote))

    def follow_market(self, base, quote):
        self.markets_following[OkexService._to_market(base, quote)] = {'base': base, 'quote': quote}
        self.order_book_socket.add_subscription(OkexService._to_market(base, quote))

    def get_balances_spot_acc(self):
        response = self.rest_client.user_info()
        if response['result'] is True:
            internal_balances = []
            entries = response['info']['funds']['free'].keys()
            for entry in entries:
                asset = entry
                locked = Decimal(str(response['info']['funds']['freezed'][entry]))
                free = Decimal(str(response['info']['funds']['free'][entry]))
                if (locked > Decimal(0)) or (free > Decimal(0)):
                    internal_balances.append({'asset': asset, 'free': free, 'locked': locked})

            self.notify_callbacks('account', account_type='balance', data=internal_balances)

            return internal_balances
        else:
            self.notify_callbacks('account', account_type='balances_failed', data={})
            return []

    def get_balances(self):
        response = self.rest_client.wallet_info()
        if response['result'] is True:
            internal_balances = []
            entries = response['info']['funds']['free'].keys()
            for entry in entries:
                asset = entry
                locked = Decimal(str(response['info']['funds']['holds'][entry]))
                free = Decimal(str(response['info']['funds']['free'][entry]))
                if (locked > Decimal(0)) or (free > Decimal(0)):
                    internal_balances.append({'asset': asset, 'free': free, 'locked': locked})

            self.notify_callbacks('account', account_type='balance', data=internal_balances)

            return internal_balances
        else:
            self.notify_callbacks('account', account_type='balances_failed', data={})
            return []

    def unfollow_all(self):
        markets = self.order_book_socket.books_following.keys()

        for market in markets:
            self.order_book_socket.remove_subscription(market)

    def withdraw(self, currency, amount, address, tag=None, cb=None, **kwargs):
        # TODO - internal format & cb
        resp = self.rest_client.withdraw(symbol=currency, trade_pwd=kwargs['withdraw_password'],\
                                  withdraw_address=address, withdraw_amount=amount)
        if resp is None:
            print('withdraw failed for unknown reason.')
        else:
            return resp

    def can_deposit(self, currency):
        return False

    def can_withdraw(self, currency):
        return True   # only through REST

    def get_withdrawals(self, currency=None):
        currency = OkexService._single_currency_symbol(currency)
        return self.rest_client.deposit_withdraw_record(symbol=currency, dw_type=1, current_page=1, page_length=50)

    def get_deposits(self, currency=None):
        currency = OkexService._single_currency_symbol(currency)
        return self.rest_client.deposit_withdraw_record(symbol=currency, dw_type=0, current_page=1, page_length=50)

    def get_deposit_address(self, currency):
        pass  # deposit_withdraw_record in restclient

    def get_public_trades(self, base, quote, start_s=None, end_s=None, **kwargs):
        '''60 most recent trades'''
        data = self.rest_client.trade_history(OkexService._to_market(base, quote))
        result = []
        temp_dic_data = dict.fromkeys(self.tx_format)
        temp_dic_data['exchange'] = 'okex'
        temp_dic_data['base'] = base.upper()
        temp_dic_data['quote'] = quote.upper()

        for item in data:
            temp_dic_data['tx_id'] = item['tid']
            temp_dic_data['filled_price'] = item['price']
            temp_dic_data['quantity'] = item['amount']
            temp_dic_data['filled_time'] = to_datetime(['date_ms'], unit='s', utc=True).to_pydatetime()

            if item['type'] == 'sell':
                temp_dic_data['maker_side'] = 'buy'
                temp_dic_data['taker_side'] = 'sell'
            elif item['type'] == 'buy':
                temp_dic_data['maker_side'] = 'sell'
                temp_dic_data['taker_side'] = 'buy'

            result.append(temp_dic_data.copy())

        return result


    def get_our_trades(self, base, quote, start_s=None, end_s=None, **kwargs):
        if 'page_no' in kwargs.keys():
            data = self.rest_client.get_orders_info_bysymbol((OkexService._to_market(base, quote)), status=1,
                                                             current_page=kwargs['page_no'])
        else:
            total_pages = self.rest_client.get_orders_info_bysymbol((OkexService._to_market(base, quote)),
                                                                    status=1)['total']
            data = []
            for page in range(1, total_pages):
                data = data + self.rest_client.get_orders_info_bysymbol((OkexService._to_market(base, quote)), status=1,
                                                                        current_page=page)['orders']
        result = []
        temp_dic_data = dict.fromkeys(self.tx_format)
        temp_dic_data['exchange'] = 'okex'
        temp_dic_data['base'] = base.upper()
        temp_dic_data['quote'] = quote.upper()
        temp_dic_data['is_our_trade'] = True

        for item in data:
            temp_dic_data['filled_price'] = item['avg_price']
            temp_dic_data['quantity'] = item['deal_amount']
            temp_dic_data['filled_time'] = to_datetime(['create_date'], unit='s', utc=True).to_pydatetime()

            if item['status'] == 1:
                temp_dic_data['fill_type'] = 'PARTIAL_FILL'
            elif item['status'] == 2:
                temp_dic_data['fill_type'] = 'FILL'

            if item['type'] == 'sell':
                temp_dic_data['maker_side'] = 'sell'
                temp_dic_data['taker_side'] = 'buy'
                temp_dic_data['our_trade_side'] = 'sell'
            elif item['type'] == 'buy':
                temp_dic_data['maker_side'] = 'buy'
                temp_dic_data['taker_side'] = 'sell'
                temp_dic_data['our_trade_side'] = 'buy'
            elif item['type'] == 'sell_market':
                temp_dic_data['maker_side'] = 'buy'
                temp_dic_data['taker_side'] = 'sell'
                temp_dic_data['our_trade_side'] = 'sell'
            elif item['type'] == 'buy_market':
                temp_dic_data['maker_side'] = 'sell'
                temp_dic_data['taker_side'] = 'buy'
                temp_dic_data['our_trade_side'] = 'buy'

            string = ''  # Creating hash of all fields for tx_id
            for i in temp_dic_data.values():
                string = string + str(i)
            hash_obj = hashlib.sha1(string.encode())

            temp_dic_data['tx_id'] = hash_obj.hexdigest()


            result.append(temp_dic_data.copy())

        return result

    def wallet_trading_fund_transfer(self, currency, amount, from_acc, to_acc):
        '''
        Transfer fund between spot, future and wallet account, funds availabe in wallet account are not
        available to trade
        :param currency: E.g. 'btc'
        :param amount: Float, E.g. 0.003
        :param from_acc: 1: spot, 3: future, 6: my wallet
        :param to_acc: 1: spot, 3: future, 6: my wallet
        :return: Response from OKEX REST API
        '''
        currency = OkexService._single_currency_symbol(currency)
        return self.rest_client.internal_fund_transfer(symbol=currency, amount=amount, from_acc=from_acc,
                                                       to_acc=to_acc)

    @staticmethod
    def _to_market(base, quote):
        return base.lower() + '_' + quote.lower()

    @staticmethod
    def _single_currency_symbol(currency):
        '''
        :param currency: E.g. 'btc'
        :return: 'btc_usd'. Serve as an argument in viewing account's information
        '''
        return currency.lower() + '_' + 'usd'


if __name__ == '__main__':
    def callback(data_type, data, **unused):
        if data_type != 'order_book':
            print('data_type: ' + data_type + '. data: ' + str(data) + '. unused: ' + str(unused))
        else:
            print('book received')


    creds = get_creds()
    ok = OkexService('okex', public_key=creds['okex_pub_prod'], private_key=creds['okex_priv_prod'])
    ok.add_callback('engine', callback)
    ok.follow_market('ETH', 'BTC')
    ok.get_order_book('ETH', 'BTC')
    # ok.create_order('ETH', 'BTC', 0.04818836, 0.35, 'sell', 'limit', 'internal_id1')
    # ok.cancel_order('ETH', 'BTC', 'internal_id', 'request_id', exchange_order_id='fake_exchange_id')
    while True:
        sleep(10)
    else:
        pass
