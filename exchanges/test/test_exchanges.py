from exchanges.factory import CreateExchangeService

from exchanges.binance import BinanceService
from pandas import DataFrame


message = {
    'action': 'CREATED',
    'exchange': 'binance',
    'symbol': 'UBT/ETH',
    'exchange_order_id': '1',
    'internal_order_id': '2',
    'side': 'buy',
    'quantity': '10',
    'price': '100',
    'cum_quantity_filled': '5',
    'order_status': 'CREATED',
    'server_ms': 1000,
    'received_ms': 1000}

order_book = DataFrame([{'side': 'ask', 'quantity': 20.0, 'price': 120.0, 'depth': 2, 'exchange': 'binance'},
                        {'side': 'ask', 'quantity': 10.0, 'price': 110.0, 'depth': 1, 'exchange': 'binance'},
                        {'side': 'bid', 'quantity': 12.0, 'price': 100.0, 'depth': 1, 'exchange': 'binance'},
                        {'side': 'bid', 'quantity': 32.0, 'price': 90.0, 'depth': 2, 'exchange': 'binance'}])


def test_factory():
    idex = CreateExchangeService('idex', None, None, tick_tock=False)
    qryptos = CreateExchangeService('qryptos', None, None, tick_tock=False)
    binance = CreateExchangeService('binance', None, None)


if __name__ == '__main__':
    test_factory()