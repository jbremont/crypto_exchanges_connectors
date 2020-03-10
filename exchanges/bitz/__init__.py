import re
import os

import configparser as ConfigParser #py3 is ConfigParser
import ccxt

try:
    from aj_sns.creds_retriever import get_creds
    from exchanges.exchange import Exchange
except:
    #Local dev
    class Exchange(object):
        def __init__(self,name):
            self.name=name
            return


#0v2# JC Sept 24, 2018  Extend into aj package
#0v1# JC Sept 21, 2018  Setup base infrastructure


# REFERENCE:
# BIT-Z API REF:  https://apidoc.bit-z.pro/en/ 
# CCXT BIT-Z API: https://github.com/ccxt/ccxt/blob/master/python/ccxt/bitz.py 
# 

# EXTRA OPTIONS:
#The library supports concurrent asynchronous mode with asyncio and async/await in Python 3.5.3+
#import ccxt.async_support as ccxt # link against the asynchronous version of ccxt

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '.'))

Config = ConfigParser.ConfigParser()
Config.read(BASE_DIR+"/settings.ini")


class CCXT_Interface(object):
    #https://github.com/ccxt/ccxt/wiki/Manual

    def __init__(self):
        return
    
    def exchange_connect(self,eid):
        global Config
        is_authenticated=False
        exchange=getattr(ccxt,eid)
        baseObject= exchange({'apiKey':Config.get(eid,'apiKey'),'secret':Config.get(eid,'secret')})
        self.__class__ = type(baseObject.__class__.__name__, (self.__class__, baseObject.__class__), {})
        self.__dict__ = baseObject.__dict__
        is_authenticated=True
        return is_authenticated
    
    def get_commands(self):
        cmds=['load_markets, fetch_order_book, fetch_ticker, fetch_trades, fetch_balance, create_market_sell_order, create_limit_buy_order']
        return

class BitzService(Exchange):

    def __init__(self, name, public_key, private_key):
        Exchange.__init__(self, name)
        #Keep as separate instance
        self.CCXT=CCXT_Interface()
        self.is_authenticated=self.CCXT.exchange_connect(name)
        #Default info load:
        self._load_markets()
        
        
    def _load_markets(self,verbose=False):
        self.markets = self.CCXT.load_markets()
        if verbose: print ("[markets]: "+str(self.markets))
    
    #staticmethod
    def _to_market(base,quote):
        return quote+"/"+base

    def get_order_book(self, base, quote, limit=None):
        book=self.CCXT.fetch_order_book(BitzService._to_market(base,quote), limit=limit, params={}) #params:  Overrides if necessary

        internal_book = {'bids': [], 'asks': []}
        
        print ("[] use decimals, or float conversion")
        for bid in book['bids']:
            internal_book['bids'].append([str(bid[0]), str(bid[1])])
        for ask in book['asks']:
            internal_book['asks'].append([str(bid[0]), str(bid[1])])

        internal_book['base'] = base
        internal_book['quote'] = quote
        internal_book['exchange'] = self.name

        self.notify_callbacks('order_book', data=internal_book)

        return internal_book
    

    def follow_market(self, base, quote):
        #print(CC.fetch_ticker('BTC/USD'))
        pass

    def unfollow_market(self, base, quote):
        pass

    def unfollow_all(self):
        pass

    def get_balances(self):
        my_balance_dict=self.CCXT.fetch_balance()
        my_balance=my_balance_dict['info']['data']['usd']
        pass

    def create_order(self, base, quote, price, quantity, side, order_type, internal_order_id, request_id=None,
                     requester_id=None, **kwargs):
        #print(CC.id, CC.create_market_sell_order('BTC/USD', 1))
        #print(CC.id, CC.create_limit_buy_order('BTC/EUR', 1, 2500.00))
        # pass/redefine custom exchange-specific order params: type, amount, price, flags, etc...
        #CC.create_market_buy_order('BTC/USD', 1, {'trading_agreement': 'agree'})
        pass

    def cancel_order(self, base, quote, internal_order_id, request_id, requester_id=None, exchange_order_id=None):
        pass

    def cancel_all(self, base, quote):
        pass

    def can_withdraw(self, currency):
        return True

    def withdraw(self, currency, amount, address, tag=None, cb=None):
        pass

    def can_deposit(self, currency):
        return True

    def get_deposit_address(self, currency):
        pass

    def get_deposits(self, currency=None):
        pass

    def get_withdrawals(self, currency):
        pass

    def get_public_trades(self, base, quote, start_s, end_s):
        #print(CC.fetch_trades('LTC/CNY'))
        pass

    def get_our_trades(self, base, quote, start_s, end_s):
        pass
#        if exchange.has['fetchOrders']:
#            since = exchange.milliseconds () - 86400000  # -1 day from now
#            # alternatively, fetch from a certain starting datetime
#            # since = exchange.parse8601('2018-01-01T00:00:00Z')
#            all_orders = []
#            while since < exchange.milliseconds ():
#                symbol = None  # change for your symbol
#                limit = 20  # change for your limit
#                orders = await exchange.fetch_orders(symbol, since, limit)
#                if len(orders):
#                    since = orders[len(orders) - 1]
#                    all_orders += orders
#                else:
#                    break

def test_interface():
    creds = get_creds()
    b = BitzService('bitz', public_key=creds['bitz_pub_prod'], private_key=creds['bitz_priv_prod'])
    b.add_callback('a_name', callback)
    b.follow_market('ETH', 'BTC')
    return

def dev_interface():
    base='BTC'
    quote='ETH'
    b = BitzService('bitz', public_key='', private_key='')
    b.get_order_book(base,quote)
    print ("Done dev_interface")
    return

if __name__ == '__main__':
    def callback(data_type, data, **unused):
        print('data_type: ' + data_type + '. data: ' + data + '. unused: ' + unused)
        
    branches=['test_interface']
    branches=['dev_interface']
    for b in branches:
        globals()[b]()
    
    
    
    
