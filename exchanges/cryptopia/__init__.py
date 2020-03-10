import threading
import traceback
from _decimal import Decimal

import time

import sys
from aj_sns.creds_retriever import get_creds
from aj_sns.log_service import logger

from exchanges.exchange import Exchange
from exchanges.cryptopia.api import Api

class CryptopiaService(Exchange):

    def __init__(self, name, public_key, private_key, poll_time_s=5, tick_tock=True):
        Exchange.__init__(self, name)
        self.open_orders_by_exchange_id = {}
        self.external_to_internal_id = {}
        self.internal_to_external_id = {}
        self.poll_time_s = poll_time_s
        self.tick_tock = True
        self.markets_following = {}
        self.client = Api(public_key, private_key)
        self.open_orders = []

        if tick_tock is True:
            threading.Timer(self.poll_time_s, self.on_tick).start()

    def get_order_book(self, base, quote):
        market = base + '_' + quote
        response = self.client.get_orders(market)
        response = response[0]
        book = {'bids': [], 'asks': []}

        for bid in response['Buy']:
            book['bids'].append([Decimal(str(bid['Price'])), Decimal(str(bid['Volume']))])
        for ask in response['Sell']:
            book['asks'].append([Decimal(str(ask['Price'])), Decimal(str(ask['Volume']))])

        book['base'] = base
        book['quote'] = quote
        book['exchange'] = self.name

        return book

    def _send_order_book_to_cb(self, base, quote):
        book = self.get_order_book(base, quote)

        self.notify_callbacks('order_book', data=book)

    def follow_market(self, base, quote):
        self.markets_following[(base, quote)] = {'base': base, 'quote': quote}

    def on_tick(self):
        try:
            logger().info('tick')
            if len(self.callbacks) > 0 and len(self.markets_following) > 0:
                for market in self.markets_following.copy().values():
                    base = market['base']
                    quote = market['quote']
                    self._send_executions_to_cb(base, quote)
                    self._send_order_book_to_cb(base, quote)
            else:
                logger().info('Not following any markets')
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            logger().error('on_tick failed with error: ' + str(e))

        logger().info('tock')

        threading.Timer(self.poll_time_s, self.on_tick).start()

    def _send_executions_to_cb(self, base, quote):
        if len(self.open_orders_by_exchange_id) == 0:
            return

        market = base + '/' + quote
        exchange_orders, error = self.client.get_openorders(market)
        unmatched_orders = {}

        for exchange_id in self.open_orders_by_exchange_id.keys():
            unmatched_orders[exchange_id] = True

        for exchange_order in exchange_orders:
            exchange_id = str(exchange_order['OrderId'])
            if exchange_id in self.open_orders_by_exchange_id:
                unmatched_orders.pop(exchange_id, None)
                open_order = self.open_orders_by_exchange_id[exchange_id]

                exchange_executed = Decimal(str(exchange_order['Amount'])) - Decimal(str(exchange_order['Remaining']))
                internal_executed = Decimal(str(open_order['cum_quantity_filled']))
                newly_executed_amount = exchange_executed - internal_executed

                if newly_executed_amount > Decimal(0):
                    open_order['cum_quantity_filled'] = Decimal(str(open_order['cum_quantity_filled'])) + \
                                                        newly_executed_amount

                    message = {
                        'action': 'EXECUTION',
                        'exchange': self.name,
                        'base': base,
                        'quote': quote,
                        'exchange_order_id': str(open_order['exchange_order_id']),
                        'internal_order_id': str(open_order['internal_order_id']),
                        'side': open_order['side'],
                        'quantity': open_order['quantity'],
                        'price': open_order['price'],
                        'cum_quantity_filled': open_order['cum_quantity_filled'],
                        'order_status': 'PARTIALLY_FILLED',
                        'server_ms': int(round(time.time() * 1000)),
                        'received_ms': int(round(time.time() * 1000)),
                        'last_executed_quantity': newly_executed_amount,
                        'last_executed_price': open_order['price'],
                        'fee_base': Decimal('0'),
                        'fee_quote': Decimal('0'),
                        'trade_id': '-1'
                    }

                    self.notify_callbacks('trade_lifecycle', trade_lifecycle_type=message['action'], data=message)

        # If it's unmatched, the exchange has it closed and us open, so it must've been filled
        five_seconds_ago_in_ms = (time.time() - 5) * 1000

        for unmatched_order_exchange_id in unmatched_orders.keys():
            open_order = self.open_orders_by_exchange_id[unmatched_order_exchange_id]
            if open_order['received_ms'] < five_seconds_ago_in_ms and \
                            market == open_order['base'] + '/' + open_order['quote']:
                newly_executed_amount = Decimal(str(open_order['quantity'])) - \
                                        Decimal(str(open_order['cum_quantity_filled']))
                open_order['cum_quantity_filled'] = Decimal(str(open_order['quantity']))

                message = {
                    'action': 'EXECUTION',
                    'exchange': self.name,
                    'base': base,
                    'quote': quote,
                    'exchange_order_id': str(open_order['exchange_order_id']),
                    'internal_order_id': str(open_order['internal_order_id']),
                    'side': open_order['side'],
                    'quantity': open_order['quantity'],
                    'price': open_order['price'],
                    'cum_quantity_filled': open_order['cum_quantity_filled'],
                    'order_status': 'FILLED',
                    'server_ms': int(round(time.time() * 1000)),
                    'received_ms': int(round(time.time() * 1000)),
                    'last_executed_quantity': newly_executed_amount,
                    'last_executed_price': open_order['price'],
                    'fee_base': Decimal('0'),
                    'fee_quote': Decimal('0'),
                    'trade_id': '-1'
                }

                self.internal_to_external_id.pop(str(open_order['internal_order_id']), None)
                self.external_to_internal_id.pop(str(open_order['exchange_order_id']), None)
                self.open_orders_by_exchange_id.pop(str(str(open_order['exchange_order_id'])), None)

                self.notify_callbacks('trade_lifecycle', trade_lifecycle_type=message['action'], data=message)

    def unfollow_market(self, base, quote):
        self.markets_following.pop((base, quote), None)

    def unfollow_all(self):
        self.markets_following = {}

    def get_balances(self):
        balances = self.client.get_balance(None)[0]

        internal_balances_format = []

        for balance in balances:
            free = Decimal(str(balance['Available']))
            locked = Decimal(str(balance['HeldForTrades']))
            if free + locked > Decimal('0'):
                internal_balances_format.append({
                    'asset': balance['Symbol'],
                    'free': free,
                    'locked': locked
                })

        self.notify_callbacks('account', account_type='balance', data=internal_balances_format)

        return internal_balances_format

    def create_order(self, base, quote, price, quantity, side, order_type, internal_order_id, request_id=None,
                     requester_id=None, **kwargs):
        market = base + '/' + quote
        response, error = self.client.submit_trade(market, side, str(price), str(quantity))

        if error is not None:
            self.notify_callbacks('trade_lifecycle', data={
                'action': 'CREATE_FAILED',
                'reason': 'Unknown exception type',
                'exchange': self.name,
                'base': base,
                'quote': quote,
                'internal_order_id': str(internal_order_id),
                'side': side,
                'quantity': quantity,
                'price': price,
                'cum_quantity_filled': 0,
                'received_ms': int(round(time.time() * 1000))
            })
            logger().error('Failed to create cryptopia order with error: {}'.format(str(error)))
            return

        exchange_order_id = response['OrderId']

        self.internal_to_external_id[str(internal_order_id)] = str(exchange_order_id)
        self.external_to_internal_id[str(exchange_order_id)] = str(internal_order_id)

        internal_response = {
            'action': 'CREATED',
            'exchange': self.name,
            'base': base,
            'quote': quote,
            'exchange_order_id': str(exchange_order_id),
            'internal_order_id': str(internal_order_id),
            'side': side,
            'quantity': Decimal(str(quantity)),
            'price': Decimal(str(price)),
            'cum_quantity_filled': Decimal('0'),
            'order_status': 'OPEN',
            'server_ms': int(round(time.time() * 1000)),
            'received_ms': int(round(time.time() * 1000))
        }
        self.open_orders_by_exchange_id[str(exchange_order_id)] = internal_response
        self.notify_callbacks('trade_lifecycle', data=internal_response)

    def cancel_order(self, base, quote, internal_order_id, request_id, requester_id=None, exchange_order_id=None):
        try:
            if exchange_order_id is None:
                exchange_order_id = self.get_exchange_id(internal_order_id)
        except LookupError:
            self.notify_callbacks('trade_lifecycle', data={
                'action': 'CANCEL_FAILED',
                'reason': 'order_not_found',
                'base': base,
                'quote': quote,
                'exchange': self.name,
                'exchange_order_id': str(exchange_order_id),
                'internal_order_id': str(internal_order_id),
                'order_status': 'UNKNOWN',
                'server_ms': int(round(time.time() * 1000)),
                'received_ms': int(round(time.time() * 1000))
            })
            return

        resp, error = self.client.cancel_trade('Trade', exchange_order_id, None)

        if error is not None:
            logger().error('Failed to cancel order with error: {}'.format(error))
            reason = str(error)
            if error == 'No matching trades found':
                reason = 'order_not_found'
                self.internal_to_external_id.pop(str(internal_order_id), None)
                self.external_to_internal_id.pop(str(exchange_order_id), None)
                self.open_orders_by_exchange_id.pop(str(exchange_order_id), None)

            self.notify_callbacks('trade_lifecycle', data={
                'action': 'CANCEL_FAILED',
                'reason': reason,
                'base': base,
                'quote': quote,
                'exchange': self.name,
                'exchange_order_id': str(exchange_order_id),
                'internal_order_id': str(internal_order_id),
                'order_status': 'UNKNOWN',
                'server_ms': int(round(time.time() * 1000)),
                'received_ms': int(round(time.time() * 1000))
            })

            return

        self.internal_to_external_id.pop(str(internal_order_id), None)
        self.external_to_internal_id.pop(str(exchange_order_id), None)
        self.open_orders_by_exchange_id.pop(str(exchange_order_id), None)

        time.sleep(2)

        self.notify_callbacks('trade_lifecycle', data={
            'action': 'CANCELED',
            'exchange': self.name,
            'base': base,
            'quote': quote,
            'exchange_order_id': str(exchange_order_id),
            'internal_order_id': str(internal_order_id),
            'order_status': 'CANCELED',
            'server_ms': int(round(time.time() * 1000)),
            'received_ms': int(round(time.time() * 1000))
        })

    def get_exchange_id(self, internal_id):
        if internal_id in self.internal_to_external_id:
            return self.internal_to_external_id[internal_id]

        raise LookupError('Could not find open order with internal id: {}'.format(internal_id))

    def cancel_all(self, base, quote):
        market = base + '/' + quote
        exchange_orders, error = self.client.get_openorders(market)
        for exchange_order in exchange_orders:
            try:
                exchange_order_id = str(exchange_order['OrderId'])
                resp, error = self.client.cancel_trade('Trade', exchange_order_id, None)
                if exchange_order_id in self.open_orders_by_exchange_id:
                    self.open_orders_by_exchange_id.pop(exchange_order_id, None)
                    internal_id = self.external_to_internal_id.pop(exchange_order, None)
                    self.internal_to_external_id.pop(internal_id, None)
                if error is not None:
                    raise Exception('Failed to cancel order with reason: {}'.format(str(error)))
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
                logger().error('cancel_all failed with error: ' + str(e))

    def can_withdraw(self, currency):
        return False

    def withdraw(self, currency, amount, address, tag=None, cb=None, **kwargs):
        pass

    def can_deposit(self, currency):
        return False

    def get_deposit_address(self, currency):
        response, error = self.client.get_deposit_address(currency)
        return response['Address']

    def get_deposits(self, currency=None):
        pass

    def get_withdrawals(self, currency):
        pass

    def get_public_trades(self, base, quote, start_s, end_s):
        pass

    def get_our_trades(self, base, quote, start_s, end_s):
        market = base + '_' + quote
        trade_history = self.client.get_tradehistory(market, count=10000)
        return trade_history

    def get_open_orders_by_side_and_price(self):
        open_orders_by_side_and_price = {'bids': {}, 'asks': {}}

        for order in self.open_orders_by_exchange_id.values():
            if order['side'] == 'buy':
                if Decimal(order['price']) in open_orders_by_side_and_price['bids']:
                    open_orders_by_side_and_price['bids'][Decimal(order['price'])] \
                        = open_orders_by_side_and_price['bids'][order['price']] + \
                          order['quantity'] - order['cum_quantity_filled']
                else:
                    open_orders_by_side_and_price['bids'][Decimal(order['price'])] = order['quantity'] - \
                                                                                     order['cum_quantity_filled']
            elif order['side'] == 'sell':
                if Decimal(order['price']) in open_orders_by_side_and_price['asks']:
                    open_orders_by_side_and_price['asks'][Decimal(order['price'])] \
                        = open_orders_by_side_and_price['asks'][order['price']] + \
                          order['quantity'] - order['cum_quantity_filled']
                else:
                    open_orders_by_side_and_price['asks'][Decimal(order['price'])] = order['quantity'] - \
                                                                                     order['cum_quantity_filled']

        return open_orders_by_side_and_price


if __name__ == '__main__':
    def callback(data_type, data, **unused):
        print('data_type: ' + str(data_type) + '. data: ' + str(data) + '. unused: ' + str(unused))

    creds = get_creds()
    b = CryptopiaService('cryptopia', public_key=creds['cryptopia_pub_prod'], private_key=creds['cryptopia_priv_prod'])
    b.add_callback('a_name', callback)

    import csv

    toCSV, error = b.get_our_trades('UBT', 'BTC', 0, 999999999999)
    keys = toCSV[0].keys()

    with open('cryptopia_executions_ours.csv', 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(toCSV)
    #b.follow_market('ETH', 'BTC')
    # b.get_balances()
    # {'OrderId': 1478549856, 'FilledOrders': []}
    # 1478554373
    #b.create_order('ETH', 'BTC', '0.041016', '0.1', 'sell', 'limit', 'internal_id')
    # b.cancel_order('ETH', 'BTC', 'internal_id1', 'request_id1', exchange_order_id=str(1478554373))
    # b.get_deposit_address('ETH')
    while True:
        time.sleep(10)
    else:
        pass
