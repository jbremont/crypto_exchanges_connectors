import traceback

import sys
from aj_sns.log_service import logger
from bittrex_websocket import OrderBook
from time import time

from exchanges.exchange import Exchange


class OrderBookSocket(OrderBook):

    def __init__(self, owner):
        OrderBook.__init__(self)
        self.owner = owner
        self.books_following = {}

    def add_subscription(self, market):
        if market not in self.books_following:
            self.books_following[market] = True
            self.subscribe_to_orderbook([market])

    def remove_subscription(self, market):
        self.books_following.pop(market, None)

    def on_ping(self, msg):
        try:
            if msg in self.books_following:
                book = self.get_order_book(msg)
                internal_book = {'bids': [], 'asks': []}

                bids = book['Z']
                asks = book['S']

                for bid in bids:
                    internal_book['bids'].append([str(bid['R']), str(bid['Q'])])
                for ask in asks:
                    internal_book['asks'].append([str(ask['R']), str(ask['Q'])])

                base, quote = OrderBookSocket.to_base_quote(msg)
                internal_book['base'] = base
                internal_book['quote'] = quote
                internal_book['exchange'] = 'bittrex'

                self.owner.notify_callbacks('order_book', data=internal_book)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            logger().error('bittrex order book socket failed with error: ' + str(e))


    @staticmethod
    def to_base_quote(market):
        parts = market.split('-')
        base = parts[1]
        quote = parts[0]
        return base, quote
