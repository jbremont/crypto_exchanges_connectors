import traceback
from _decimal import Decimal
from time import time

import sys
from aj_sns.log_service import logger
from bittrex_websocket import BittrexSocket


class ExecutionsSocket(BittrexSocket):

    def __init__(self, owner):
        BittrexSocket.__init__(self)
        self.owner = owner
        self.markets_following = {}

    def add_subscription(self, market):
        if market not in self.markets_following:
            self.markets_following[market] = True

    def remove_subscription(self, market):
        self.markets_following.pop(market, None)

    def on_public(self, msg):
        pass

    def on_private(self, msg):
        try:
            if 'TY' in msg:
                update_type = msg['TY']
                if update_type == 1 or update_type == 2:
                    order = msg['o']
                    market = order['E']
                    base, quote = ExecutionsSocket.to_base_quote(market)

                    if market in self.markets_following:
                        exchange_order_id = str(order['OU'])
                        internal_order = self.owner.get_order_by_exchange_id(exchange_order_id)
                        if internal_order is not None:
                            quantity = Decimal(str(order['Q']))
                            quantity_remaining = Decimal(str(order['q']))
                            price = str(order['PU'])
                            if update_type == 1:
                                status = 'PARTIALLY_FILLED'
                            else:
                                status = 'FILLED'
                            new_fill_amount = str(quantity - quantity_remaining -
                                                  Decimal(str(internal_order['cum_quantity_filled'])))
                            internal_order['cum_quantity_filled'] = str(Decimal(str(internal_order['quantity'])) -
                                                                        quantity_remaining)
                            message = {
                                'action': 'EXECUTION',
                                'exchange': self.owner.name,
                                'base': base,
                                'quote': quote,
                                'exchange_order_id': str(internal_order['exchange_order_id']),
                                'internal_order_id': str(internal_order['internal_order_id']),
                                'side': internal_order['side'],
                                'quantity': internal_order['quantity'],
                                'price': internal_order['price'],
                                'cum_quantity_filled': internal_order['cum_quantity_filled'],
                                'order_status': status,
                                'server_ms': int(round(time() * 1000)),
                                'received_ms': int(round(time() * 1000)),
                                'last_executed_quantity': new_fill_amount,
                                'last_executed_price': price,
                                'fee_base': 0,
                                'fee_quote': 0,
                                'trade_id': '-1'
                            }

                            if status == 'FILLED':
                                index_to_del = None
                                i = 0

                                for order in self.owner.open_orders:
                                    if order['exchange_order_id'] == exchange_order_id:
                                        index_to_del = i
                                        break
                                    i += 1

                                if index_to_del is not None:
                                    self.owner.open_orders.pop(index_to_del)

                            self.owner.notify_callbacks('trade_lifecycle', trade_lifecycle_type=message['action'], data=message)
                        else:
                            logger().error('Failed to get order with exchange id: ' + exchange_order_id)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            logger().error('bittrex execution socket failed with error: ' + str(e))



    @staticmethod
    def to_base_quote(market):
        parts = market.split('-')
        base = parts[1]
        quote = parts[0]
        return base, quote