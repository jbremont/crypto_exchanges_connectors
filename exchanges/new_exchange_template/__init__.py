from aj_sns.creds_retriever import get_creds

from exchanges.exchange import Exchange


class BittrexService(Exchange):

    def __init__(self, name, public_key, private_key):
        Exchange.__init__(self, name)

    def get_order_book(self, base, quote):
        pass

    def follow_market(self, base, quote):
        pass

    def unfollow_market(self, base, quote):
        pass

    def unfollow_all(self):
        pass

    def get_balances(self):
        pass

    def create_order(self, base, quote, price, quantity, side, order_type, internal_order_id, request_id=None,
                     requester_id=None, **kwargs):
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
        pass

    def get_our_trades(self, base, quote, start_s, end_s):
        pass


if __name__ == '__main__':
    def callback(data_type, data, **unused):
        print('data_type: ' + data_type + '. data: ' + data + '. unused: ' + unused)

    creds = get_creds()
    b = BittrexService('bittrex', public_key=creds['bittrex_pub_prod'], private_key=creds['bittrex_priv_prod'])
    b.add_callback('a_name', callback)
    b.follow_market('ETH', 'BTC')
