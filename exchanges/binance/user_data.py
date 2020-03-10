import traceback
from _decimal import Decimal

import sys

from aj_sns.log_service import logger
from binance.websockets import BinanceSocketManager
import time

from exchanges.common.open_order_tracker import OrderTracker


class UserDataService(OrderTracker):

    def __init__(self, binance_client, callback, name):
        OrderTracker.__init__(self)
        self.client = binance_client
        self.callback = callback
        self.bm = BinanceSocketManager(self.client)
        self.conn_key = None
        self.name = name
        
    def start(self):
        self.conn_key = self.bm.start_user_socket(self.__process_user_data)
        self.bm.start()

    def stop(self):
        #TODO figure out how to split code so that you can call start, stop, start etc.
        if self.conn_key is not None:
            self.bm.stop_socket(self.conn_key)
        self.bm.close()

    def get_open_orders(self):
        return self.open_orders.copy()

    def __process_user_data(self, event):
        try:
            if event['e'] == 'executionReport':
                symbol = event['s']
                base = symbol[0:3]
                quote = symbol[3:]  # TODO - Do this properly, so SALT/USDT would work
                message = {
                    'action': 'UNKNOWN',
                    'exchange': self.name,
                    'symbol': event['s'],
                    'base': base,
                    'quote': quote,
                    'exchange_order_id': event['i'],
                    'internal_order_id': event['c'],
                    'side': str.lower(event['S']),
                    'quantity': event['q'],
                    'price': event['p'],
                    'cum_quantity_filled': event['z'],
                    'order_status': event['X'],
                    'server_ms': event['T'],
                    'received_ms': int(round(time.time() * 1000))
                }

                if event['x'] == 'TRADE':
                    message['action'] = 'EXECUTION'
                    message['last_executed_quantity'] = event['l']
                    message['last_executed_price'] = event['L']
                    message['trade_id'] = event['t']

                    # TODO Check this logic is correct (both symbol split and if fee is an amount or a percent)
                    commission_amount = event['n']
                    commission_asset = event['N']

                    if commission_asset == base:
                        fee_base = Decimal(commission_amount)
                        fee_quote = Decimal('0')
                        message['fee_base'] = fee_base
                        message['fee_quote'] = fee_quote
                    if commission_asset == quote:
                        fee_base = Decimal('0')
                        fee_quote = Decimal(commission_amount)
                        message['fee_base'] = fee_base
                        message['fee_quote'] = fee_quote

                    if message['cum_quantity_filled'] == message['quantity']:
                        self.open_orders.pop(message['exchange_order_id'], None)
                        self.pending_cancel.pop(message['internal_order_id'], None)
                        self.internal_to_external_id.pop(message['internal_order_id'], None)
                elif event['x'] == 'NEW':
                    self.open_orders[message['exchange_order_id']] = message
                    self.internal_to_external_id[message['internal_order_id']] = message['exchange_order_id']
                    message['action'] = 'CREATED'
                elif event['x'] == 'CANCELED':
                    self.open_orders.pop(message['exchange_order_id'], None)
                    self.pending_cancel.pop(message['internal_order_id'], None)
                    self.internal_to_external_id.pop(message['internal_order_id'], None)
                    message['action'] = 'CANCELED'
                elif event['x'] == 'REJECTED':
                    message['action'] = 'REJECTED'
                    # TODO map to internal error code
                    message['rejected_reason'] = event['r']
                elif event['x'] == 'EXPIRED':
                    message['action'] = 'EXPIRED'

                self.callback('trade_lifecycle', trade_lifecycle_type=message['action'], data=message)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            logger().error('__process_user_data failed with error: ' + str(e))
