from _decimal import Decimal

from bittrex.bittrex import Bittrex, API_V1_1
from time import sleep, time

from aj_sns.creds_retriever import get_creds

from exchanges.bittrex2.executions_socket import ExecutionsSocket
from exchanges.bittrex2.order_book_socket import OrderBookSocket
from exchanges.exchange import Exchange


class BittrexService(Exchange):

    def __init__(self, name, public_key, private_key):
        Exchange.__init__(self, name)

        self.ob_ws = OrderBookSocket(self)
        self.ex_ws = ExecutionsSocket(self)
        self.ex_ws.authenticate(public_key, private_key)
        self.markets_following = {}
        self.open_orders = []
        self.rest_client = Bittrex(public_key, private_key, api_version=API_V1_1)

    def get_order_book(self, base, quote):
        resp = self.rest_client.get_orderbook(BittrexService._to_market(base, quote))
        print('rest book:' + str(resp['result']))
        if resp['success'] is True:
            book = resp['result']

            internal_book = {'bids': [], 'asks': []}

            for bid in book['buy']:
                internal_book['bids'].append([str(bid['Rate']), str(bid['Quantity'])])
            for ask in book['sell']:
                internal_book['asks'].append([str(ask['Rate']), str(ask['Quantity'])])

            internal_book['base'] = base
            internal_book['quote'] = quote
            internal_book['exchange'] = self.name

            self.notify_callbacks('order_book', data=internal_book)

            return internal_book

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

    def follow_market(self, base, quote):
        self.ob_ws.add_subscription(BittrexService._to_market(base, quote))   # bittrex has it backwards
        self.ex_ws.add_subscription(BittrexService._to_market(base, quote))   # bittrex has it backwards
        self.markets_following[self._to_market(base, quote)] = {'base': base, 'quote': quote}

    def unfollow_market(self, base, quote):
        self.ob_ws.remove_subscription(BittrexService._to_market(base, quote))
        self.ex_ws.remove_subscription(BittrexService._to_market(base, quote))
        self.markets_following.pop(self._to_market(base, quote), None)

    @staticmethod
    def _to_market(base, quote):
        return quote + '-' + base

    def unfollow_all(self):
        markets = self.ob_ws.books_following.keys()

        for market in markets:
            self.ob_ws.remove_subscription(market)

    def get_balances(self):
        response = self.rest_client.get_balances()

        if response['success'] is True:
            internal_balances = []
            entries = response['result']
            for entry in entries:
                asset = entry['Currency']
                balance = Decimal(str(entry['Balance']))
                free = Decimal(str(entry['Available']))
                if balance > Decimal(0):
                    internal_balances.append({'asset': asset, 'free': free, 'locked': balance - free})

            self.notify_callbacks('account', account_type='balance', data=internal_balances)

            return internal_balances
        else:
            self.notify_callbacks('account', account_type='balances_failed', data={})
            return []

    def create_order(self, base, quote, price, quantity, side, order_type, internal_order_id, request_id=None,
                     requester_id=None, **kwargs):
        if side == 'buy':
            response = self.rest_client.buy_limit(BittrexService._to_market(base, quote), quantity, price)
        elif side == 'sell':
            response = self.rest_client.sell_limit(BittrexService._to_market(base, quote), quantity, price)
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

        elif 'success' in response and response['success'] is False:
            reason = 'UNKNOWN'
            if response['message'] == 'INSUFFICIENT_FUNDS':
                reason = response['message']
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
        else:
            exchange_id = response['result']['uuid']
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

        response = self.rest_client.cancel(exchange_order_id)

        if response['success'] is True:
            i = 0

            if index_to_pop is None:
                for order in self.open_orders:
                    if order['exchange_order_id'] == exchange_order_id:
                        index_to_pop = i
                        break
                    i += 1

            if index_to_pop is not None:
                self.open_orders.pop(index_to_pop)

            self.notify_callbacks('trade_lifecycle', data={
                'action': 'CANCELED',
                'base': base,
                'quote': quote,
                'exchange': self.name,
                'exchange_order_id': exchange_order_id,
                'internal_order_id': internal_order_id,
                'order_status': 'CANCELED',
                'server_ms': int(round(time() * 1000)),
                'received_ms': int(round(time() * 1000))
            })
        else:
            reason = response['message']
            if response['message'] == 'INVALID_ORDER' or\
               response['message'] == 'ORDER_NOT_OPEN' or\
               response['message'] == 'UUID_INVALID':
                reason = 'order_not_found'

            self.notify_callbacks('trade_lifecycle', data={
                'action': 'CANCEL_FAILED',
                'base': base,
                'quote': quote,
                'reason': reason,
                'exchange': self.name,
                'exchange_order_id': exchange_order_id,
                'internal_order_id': internal_order_id,
                'order_status': 'UNKNOWN',
                'server_ms': int(round(time() * 1000)),
                'received_ms': int(round(time() * 1000))
            })

    def cancel_all(self, base, quote):
        open_orders_resp = self.rest_client.get_open_orders(BittrexService._to_market(base, quote))
        open_orders = open_orders_resp['result']
        for open_order in open_orders:

            exchange_order_id = open_order['OrderUuid']
            internal_order_id = None

            for order in self.open_orders:
                if order['exchange_order_id'] == exchange_order_id:
                    internal_order_id = order['internal_order_id']
                    break

            self.cancel_order(base, quote, internal_order_id, 'a_request_id', exchange_order_id=exchange_order_id)

    def can_withdraw(self, currency):
        return True

    def withdraw(self, currency, amount, address, tag=None, cb=None, **kwargs):
        # TODO - Internal format and cb
        self.rest_client.withdraw(currency, address, amount)

    def can_deposit(self, currency):
        return True

    def get_deposit_address(self, currency):
        response = self.rest_client.get_deposit_address(currency)
        return response['result']['Address']

    def get_deposits(self, currency=None):
        # TODO - Internal format
        return self.rest_client.get_deposit_history(currency)

    def get_withdrawals(self, currency):
        # TODO - Internal format
        return self.rest_client.get_withdrawal_history(currency)

    def get_order_by_exchange_id(self, exchange_id):
        for open_order in self.open_orders.copy():
            if open_order['exchange_order_id'] == exchange_id:
                return open_order

        return None

    def get_public_trades(self, base, quote, start_s, end_s):
        pass

    def get_our_trades(self, base, quote, start_s, end_s):
        pass

if __name__ == '__main__':
    def callback(data_type, data, **unused):
        if data_type != 'order_book':
            print('data_type: ' + data_type + '. data: ' + str(data) + '. unused: ' + str(unused))
        else:
            print('book received')

    creds = get_creds()
    b = BittrexService('bittrex', public_key=creds['bittrex_pub_prod'], private_key=creds['bittrex_priv_prod'])
    b.add_callback('engine', callback)
    b.follow_market('ETH', 'BTC')
    # b.get_deposit_address('ETH')
    # b.create_order('ETH', 'BTC', 0.04818836, 0.35, 'sell', 'limit', 'internal_id1')
    # b.cancel_order('ETH', 'BTC', 'internal_id', 'request_id', exchange_order_id='fake_exchange_id')
    while True:
        sleep(10)
    else:
        pass
