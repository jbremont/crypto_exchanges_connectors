from exchanges.binance import BinanceService
from exchanges.idex import IdexService


def get_service_by_name(name, public_key=None, private_key=None, **args):
    poll_time_s = args['poll_time_s'] if 'poll_time_s' in args.keys() else 5
    tick_tock = args['tick_tock'] if 'tick_tock' in args.keys() else True

    if name == 'binance':
        return BinanceService(public_key=public_key, private_key=private_key)

    if name == 'idex':
        return IdexService(address=public_key, private_key=private_key, poll_time_s=poll_time_s, tick_tock=tick_tock)

    raise NameError('Exchange by this name ({}) does not exist'.format(name))