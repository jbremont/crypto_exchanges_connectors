from exchanges.cryptopia import CryptopiaService
from exchanges.binance import BinanceService
from exchanges.idex import IdexService
from exchanges.bittrex2 import BittrexService
import ccxt

from exchanges.qryptos import QryptosService

dispatch = {'binance': {'constructor': BinanceService, 'args': {}},
            'bittrex': {'constructor': BittrexService, 'args': {'poll_time_s': 5, 'tick_tock': True}},
            'cryptopia': {'constructor': CryptopiaService, 'args': {'poll_time_s': 5, 'tick_tock': True}},
            'idex': {'constructor': IdexService, 'args': {'poll_time_s': 5, 'tick_tock': True}},
            'qryptos': {'constructor': QryptosService, 'args': {'poll_time_s': 5, 'tick_tock': True}}}


def CreateExchangeService(exchange, public_key, private_key, **args):
    if exchange in dispatch:
        params = dispatch[exchange]
        constructor = params['constructor']
        default_args = params['args']
        my_args = {}
        for key in default_args.keys():
            if key in args:
                my_args[key] = args[key]
            else:
                my_args[key] = default_args[key]
        return constructor(exchange, public_key, private_key, **args)
    else:
        try:
            return getattr(ccxt, exchange)
        except:
            raise NotImplementedError(exchange, 'has no exchange service implemented')
