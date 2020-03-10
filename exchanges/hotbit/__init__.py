import requests
import time
from pandas import DataFrame, concat, to_numeric

__version__ = '0.0.1'

class Client(object):

    def __init__(self):
        self.session = self._init_session()

    def _init_session(self):
        session = requests.session()
        headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                   'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36',
                   'Cache-Control': 'max-age=0'}
        session.headers.update(headers)
        return session

    def start(self):
        pass

    def create_order(self, base, quote, price, quantity, side, internal_order_id, retries=0, **kwargs):
        pass

    def cancel_order(self, base, quote, internal_order_id, request_id, retries=0, exchange_order_id=None, cb=True):
        pass

    def get_order_book(self, base, quote):
        response = getattr(self.session, 'get')('https://www.hotbit.io/public/order/depth?market={}{}&prec=1e-8'.format(base, quote))
        book = response.json()['Content']
        return book

if __name__ == '__main__':
    c = Client()
    book = c.get_order_book('UBT', 'ETH')
    print(book)