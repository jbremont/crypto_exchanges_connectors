import threading
import time
import traceback
import sys
from aj_sns.creds_retriever import get_creds
from decimal import Decimal
from aj_sns.log_service import logger
from quoine.client import Qryptos
from quoine.exceptions import QuoineAPIException

from exchanges.exchange import Exchange


class QryptosService(Exchange):
    def __init__(self, name, public_key, private_key, poll_time_s=5, tick_tock=True):
        Exchange.__init__(self, name)
        self.pending_cancel = {}
        self.client = Qryptos(public_key, private_key)
        self.client.API_URL = 'https://api.liquid.com'
        self.internal_to_external_id = {}
        self.external_to_internal_id = {}
        self.open_orders_by_exchange_id = {}
        self.symbol_to_product = {}
        self.markets_following = {}
        product_list = self.client.get_products()
        for product in product_list:
            if 'product_type' in product and product['product_type'] == 'CurrencyPair':
                self.symbol_to_product[product['currency_pair_code']] = product['id']

        self.poll_time_s = poll_time_s
        self.tick_tock = tick_tock
        self.name = name

        if tick_tock is True:
            threading.Timer(self.poll_time_s, self.on_tick).start()

    def can_withdraw(self, withdraw):
        return False

    def withdraw(self, currency, amount, address, tag=None, cb=None, **kwargs):
        raise NotImplementedError('Qryptos does not have a withdraw function in their API')

    def on_tick(self):
        logger().info('tick')

        if len(self.callbacks) > 0:
            for product_id, details in self.markets_following.copy().items():
                try:
                    base = details['base']
                    quote = details['quote']

                    self._send_executions_to_cb(base, quote)
                    self._send_order_book_to_cb(base, quote)
                except Exception as e:
                    traceback.print_exc(file=sys.stdout)
                    logger().error('on_tick failed with error: ' + str(e))

        logger().info('tock')
        if len(self.markets_following) > 0:
            threading.Timer(self.poll_time_s, self.on_tick).start()

    def _send_order_book_to_cb(self, base, quote):
        book = self.get_order_book(base, quote)

        self.notify_callbacks('order_book', data=book)

    def _send_executions_to_cb(self, base, quote):
        if len(self.open_orders_by_exchange_id) == 0:
            return

        for open_order in self.open_orders_by_exchange_id.copy().values():
            exchange_order = self.client.get_order(open_order['exchange_order_id'])
            newly_executed_amount = Decimal(str(exchange_order['filled_quantity'])) - \
                                    Decimal(str(open_order['cum_quantity_filled']))

            if newly_executed_amount > Decimal(0):
                open_order['cum_quantity_filled'] = Decimal(exchange_order['filled_quantity'])

                if 'fee_base' in open_order.keys():
                    fee_base_delta = Decimal(exchange_order['order_fee']) - Decimal(open_order['fee_base'])
                else:
                    fee_base_delta = Decimal(exchange_order['order_fee'])

                open_order['fee_base'] = Decimal(exchange_order['order_fee'])

                if exchange_order['status'] == 'filled':
                    status = 'FILLED'
                    self.open_orders_by_exchange_id.pop(str(exchange_order['id']))
                    self.external_to_internal_id.pop(str(open_order['exchange_order_id']))
                    self.internal_to_external_id.pop(str(open_order['internal_order_id']))
                elif exchange_order['status'] == 'cancelled':
                    status = 'CANCELED'
                    self.open_orders_by_exchange_id.pop(str(exchange_order['id']))
                    self.external_to_internal_id.pop(str(open_order['exchange_order_id']))
                    self.internal_to_external_id.pop(str(open_order['internal_order_id']))
                else:
                    status = 'PARTIALLY_FILLED'

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
                    'order_status': status,
                    'server_ms': int(round(time.time() * 1000)),
                    'received_ms': int(round(time.time() * 1000)),
                    'last_executed_quantity': newly_executed_amount,
                    'last_executed_price': open_order['price'],
                    'fee_base': fee_base_delta,
                    'fee_quote': Decimal('0'),
                    'trade_id': '-1'
                }

                self.notify_callbacks('trade_lifecycle', trade_lifecycle_type=message['action'], data=message)

    def follow_market(self, base, quote):
        product_id = self.get_product_id(base, quote)
        self.markets_following[str(product_id)] = {'base': base, 'quote': quote}

        # If this is the first market we've followed, start the tick tock
        if self.markets_following == 1 and self.tick_tock is True:
            threading.Timer(self.poll_time_s, self.on_tick).start()

    def unfollow_market(self, base, quote):
        product_id = self.get_product_id(base, quote)
        self.markets_following.pop(str(product_id), None)

    def unfollow_all(self):
        self.markets_following = {}

    def get_exchange_id(self, internal_id):
        if internal_id in self.internal_to_external_id:
            return self.internal_to_external_id[internal_id]

        raise LookupError('Could not find open order with internal id: {}'.format(internal_id))

    def cancel_order(self, base, quote, internal_order_id, request_id, requester_id=None, exchange_order_id=None):
        try:
            if exchange_order_id is None:
                exchange_order_id = self.get_exchange_id(internal_order_id)
        except LookupError:
            self.notify_callbacks('trade_lifecycle', data={
                'action': 'CANCEL_FAILED',
                'base': base,
                'quote': quote,
                'reason': 'order_not_found',
                'exchange': self.name,
                'exchange_order_id': str(exchange_order_id),
                'internal_order_id': str(internal_order_id),
                'order_status': 'UNKNOWN',
                'server_ms': int(round(time.time() * 1000)),
                'received_ms': int(round(time.time() * 1000))
            })
            return

        try:
            response = self.client.cancel_order(exchange_order_id)

            if ('message' in response and len(response['message']) > 0) or \
                    ('errors' in response and len(response['errors']) > 0):
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
                self.internal_to_external_id.pop(str(internal_order_id), None)
                self.external_to_internal_id.pop(str(exchange_order_id), None)
                self.open_orders_by_exchange_id.pop(str(exchange_order_id), None)
                return
        except QuoineAPIException as e:
            logger().error('Failed to cancel order with error: {}'.format(e))
            self.notify_callbacks('trade_lifecycle', data={
                'action': 'CANCEL_FAILED',
                'reason': 'Unknown exception type',
                'base': base,
                'quote': quote,
                'exchange': self.name,
                'exchange_order_id': str(exchange_order_id),
                'internal_order_id': str(internal_order_id),
                'order_status': 'UNKNOWN',
                'server_ms': int(round(time.time() * 1000)),
                'received_ms': int(round(time.time() * 1000))
            })
            # If fails due to "already closed" or "not found", then popping is fine
            # TODO - If it fails due to a rate limit, we probably don't want this here?
            self.internal_to_external_id.pop(str(internal_order_id), None)
            self.external_to_internal_id.pop(str(exchange_order_id), None)
            self.open_orders_by_exchange_id.pop(str(exchange_order_id), None)
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

    def get_withdrawals(self, currency):
        raise NotImplementedError('Qryptos does not have a get_withdrawals function in their API')

    def can_deposit(self, currency):
        return False

    def get_deposits(self, currency=None):
        raise NotImplementedError('Qryptos does not have a get_deposits function in their API')

    def get_order_book(self, base, quote):
        product_id = self.get_product_id(base, quote)
        book = self.client.get_order_book(product_id, full=True)
        book['bids'] = book['buy_price_levels']
        book['asks'] = book['sell_price_levels']
        book.pop('buy_price_levels', None)
        book.pop('sell_price_levels', None)

        book['base'] = base
        book['quote'] = quote
        book['exchange'] = self.name

        return book

    def get_balances(self):
        balances = self.client.get_account_balances()

        internal_balances_format = []

        for balance in balances:
            internal_balances_format.append({
                'asset': balance['currency'],
                'free': Decimal(str(balance['balance'])),
                'locked': Decimal(0)
            })

        self.notify_callbacks('account', account_type='balance', data=internal_balances_format)

        return internal_balances_format

    def create_order(self, base, quote, price, quantity, side, order_type, internal_order_id, request_id=None,
                     requester_id=None, **kwargs):
        product_id = self.get_product_id(base, quote)
        if product_id is None:
            raise LookupError('Could not find a product with a base [{}] and quote [{}]'.format(base, quote))

        try:
            exchange_side = self.client.SIDE_BUY if str.lower(side) == 'buy' else self.client.SIDE_SELL
            response = self.client.create_order(order_type, product_id, exchange_side, str(quantity), price=str(price))

            if ('message' in response and len(response['message']) > 0) or \
                    ('errors' in response and len(response['errors']) > 0):
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
                return
        except QuoineAPIException as e:
            logger().error('Failed to create order due to error: {}'.format(e))
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
            return

        self.internal_to_external_id[str(internal_order_id)] = str(response['id'])
        self.external_to_internal_id[str(response['id'])] = str(internal_order_id)

        internal_response = {
            'action': 'CREATED',
            'exchange': self.name,
            'base': base,
            'quote': quote,
            'exchange_order_id': str(response['id']),
            'internal_order_id': str(internal_order_id),
            'side': side,
            'quantity': Decimal(str(quantity)),
            'price': Decimal(str(price)),
            'cum_quantity_filled': Decimal('0'),
            'order_status': 'OPEN',
            'server_ms': response['created_at'] * 1000,
            'received_ms': int(round(time.time() * 1000))
        }
        self.open_orders_by_exchange_id[str(response['id'])] = internal_response
        self.notify_callbacks('trade_lifecycle', data=internal_response)

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

    def get_product_id(self, base, quote):
        symbol = str.upper(base) + str.upper(quote)
        if symbol in self.symbol_to_product:
            return self.symbol_to_product[symbol]
        else:
            return None

    def cancel_all(self, base, quote):
        product_id = self.get_product_id(base, quote)
        live_orders = self.client.get_orders(product_id=product_id, status='live')
        partially_filled_orders = self.client.get_orders(product_id=product_id, status='partially_filled')
        orders = live_orders['models'] + partially_filled_orders['models']

        for order in orders:
            try:
                self.client.cancel_order(order['id'])
            except Exception as e:
                logger().error('Failed to cancel order with error: {}'.format(str(e)))

        time.sleep(2)

    # Qryptos exchange fees are always paid in the quote currency
    # TODO - Use self.client.get_my_executions (with limit 1000) instead. Should be far faster
    def get_fees_paid(self, base, quote, start_s, end_s):
        if start_s > end_s:
            logger().error('Start time cannot be after end time')
            return Decimal(0)

        product_id = self.get_product_id(base, quote)
        response = self.client.get_orders(product_id=product_id, limit=100)
        # Find start page via binary search

        total_pages = self.find_actual_total_pages(product_id, response['total_pages'], response['total_pages'] + 1, 0)

        first_page = self.find_first_or_last_page_of_orders(product_id, int(total_pages / 2), start_s, end_s, 'first',
                                                            total_pages)
        if first_page == -1:
            logger().warn('Unable to find any orders on or after epoch time: {}'.format(start_s))
            return Decimal('0')

        last_page = self.find_first_or_last_page_of_orders(product_id, int(total_pages / 2), start_s, end_s, 'last',
                                                           total_pages)
        if last_page == -1:
            logger().warn('Unable to find any orders on or before epoch time: {}'.format(end_s))
            return Decimal('0')

        # Request all pages in range [start, end]
        orders_in_range_with_fills = []
        for page in range(first_page, last_page + 1):
            response = self.client.get_orders(product_id=product_id, page=page, limit=100)
            orders = response['models']
            for order in orders:
                if start_s <= order['created_at'] <= end_s and order['filled_quantity'] != '0.0':
                    orders_in_range_with_fills.append(order)

        fees_in_quote_currency = Decimal('0')
        for order in orders_in_range_with_fills:
            fees_in_quote_currency += order['order_fee']

        return fees_in_quote_currency

    def find_actual_total_pages(self, product_id, page_to_check, last_blank_page_seen, highest_non_blank_page_seen):
        response = self.client.get_orders(product_id=product_id, page=page_to_check, limit=100)
        orders = response['models']

        if len(orders) > 0:
            if last_blank_page_seen - page_to_check == 1:
                return page_to_check
            else:
                highest_non_blank_page_seen = page_to_check
                next_page_to_check = int((page_to_check + last_blank_page_seen) / 2)
                return self.find_actual_total_pages(product_id, next_page_to_check,
                                                    last_blank_page_seen, highest_non_blank_page_seen)
        else:
            last_blank_page_seen = page_to_check
            next_page_to_check = int((highest_non_blank_page_seen + last_blank_page_seen) / 2)
            return self.find_actual_total_pages(product_id, next_page_to_check, last_blank_page_seen,
                                                highest_non_blank_page_seen)

    def find_first_or_last_page_of_orders(self, product_id, current_page, start_s, end_s, first_or_last, total_pages):
        if first_or_last == 'first':
            first_page = -1
            while True:
                response = self.client.get_orders(product_id=product_id, page=current_page, limit=100)
                orders = response['models']
                matched = False
                for order in orders:
                    # Have to use created_at instead of updated_at as appears to be sorted by created_at
                    created_at_s = order['created_at']
                    if created_at_s <= end_s:
                        matched = True
                        break

                if matched:
                    first_page = current_page
                    next_page = int(current_page / 2)
                else:
                    if first_page - current_page == 1 or first_page == current_page:
                        return first_page
                    elif current_page == 0:
                        return -1
                    else:
                        next_page = int((current_page + first_page) / 2)

                if next_page == current_page:
                    return first_page
                else:
                    current_page = next_page
        else:
            last_page = -1

            while True:
                response = self.client.get_orders(product_id=product_id, page=current_page, limit=100)
                orders = response['models']
                matched = False
                for order in orders:
                    created_at_s = order['created_at']
                    if created_at_s >= start_s:
                        matched = True
                        break

                if matched:
                    last_page = current_page
                    next_page = int((current_page + total_pages) / 2)
                else:
                    if current_page == last_page + 1 or current_page == last_page:
                        return last_page
                    elif current_page == total_pages - 1:
                        return -1  # If we're on the last page and still don't have a match, it doesn't exist
                    else:
                        next_page = int((current_page + last_page) / 2)

                if next_page == current_page:
                    return last_page
                else:
                    current_page = next_page

    def get_deposit_address(self, currency):
        raise NotImplementedError('Qryptos does not have a deposit function in their API')


if __name__ == '__main__':
    def callback(data_type, data, **unused):
        print(data_type)
        print(data)


    creds = get_creds()
    q = QryptosService('qryptos', public_key=creds['qryptos_pub_prod'], private_key=creds['qryptos_priv_prod'])

    import csv

    trades = q.client.get_executions(q.get_product_id('UBT', 'ETH'), 1000, 1)
    trades = trades['models']
    trades_to_delete = []
    i = 0
    for trade in trades:
        # 1535673600 31 Aug midnight gmt
        # 1536278400 7 September midnight gmt
        if trade['created_at'] > 1536278400 or trade['created_at'] < 1535673600:
            trades_to_delete.append(i)
        i += 1

    for index in reversed(trades_to_delete):
        trades.pop(index)

    keys = trades[0].keys()

    with open('qryptos_executions_ours.csv', 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(trades)



    # order = q.client.get_order('85966628')
    # q.get_fees_paid('UBT', 'ETH', 0, 99999999999)
    # q.get_balances()
    # q.follow_market('ETH', 'BTC')
    # q.cancel_all('ETH', 'BTC')
    # price then qty
    # balances = q.get_balances()
    # print(balances)
    # q.create_order('ETH', 'BTC', '0.0452', '0.1', 'sell', 'limit', 'an_id', request_id='rid_1')
    # q.cancel_order('ETH', 'BTC', 'internal_id', 'request_id', exchange_order_id='81422113')
