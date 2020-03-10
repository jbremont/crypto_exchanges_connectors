import time
from binance.exceptions import BinanceAPIException
from aj_sns.transfer_service import TransferService
from exchanges.exchange import Exchange
from exchanges.binance.order_book import OrderBookService
from exchanges.binance.user_data import UserDataService
from binance.client import Client
from aj_sns.log_service import logger
from pandas import DataFrame


class BinanceService(Exchange, TransferService):

    def __init__(self, name, public_key=None, private_key=None):
        Exchange.__init__(self, name)
        TransferService.__init__(self)
        self.client = Client(public_key, private_key)
        self.user_data_service = UserDataService(self.client, self.notify_callbacks, name)
        self.is_authenticated = (public_key is not None) and (private_key is not None)
        self.order_book_services = {}
        self.callbacks = {}

    def follow_market(self, base, quote):
        self.follow_order_book(base, quote)
        self.follow_user_data()

    def unfollow_market(self, base, quote):
        self.unfollow_order_book(base, quote)
        if len(self.order_book_services) == 0:
            self.unfollow_user_data()

    def follow_user_data(self):
        if self.is_authenticated:
            self.user_data_service.start()

    def unfollow_user_data(self):
        if self.is_authenticated:
            self.user_data_service.stop()

    def start(self):
        pass

    def unfollow_all(self):
        for obs in self.order_book_services.values():
            obs.stop()
        self.order_book_services = {}
        self.unfollow_user_data()

    def follow_order_book(self, base, quote):
        cross = base+quote
        if cross not in self.order_book_services:
            logger().info('Subscribing to ' + cross)
            self.order_book_services[cross] = OrderBookService(self.client, base, quote,
                                                               self.notify_callbacks, self.name)
            self.order_book_services[cross].start()
            return True
        else:
            logger().warning('Already subscribed to '+base+quote)
            pass

    def unfollow_order_book(self, base, quote):
        cross = base+quote
        if cross in self.order_book_services:
            self.order_book_services[cross].stop()
            del self.order_book_services[cross]
            return True
        else:
            # send some kind of warning message (not following)?
            pass

    def get_order_book(self, base, quote):
        cross = base + quote
        if cross in self.order_book_services:
            book = self.order_book_services[cross].get_order_book()
            return book
        else:
            raise LookupError('Not subscribed to cross: {}. First call BinanceService.follow_order_book(base, quote)'
                              .format(cross))

    def create_order(self, base, quote, price, quantity, side, order_type, internal_order_id, request_id=None,
                     requester_id=None, **kwargs):
        try:
            self.client.create_order(symbol=base + quote, side=side, type=order_type, timeInForce='GTC',
                                        quantity=quantity, price=price, newClientOrderId=internal_order_id)
            message = dict()
            message['internal_order_id'] = internal_order_id
            message['request_id'] = request_id
            message['requester_id'] = requester_id
            message['action'] = 'order_sent'
            message['exchange'] = self.name
            self.notify_callbacks('trade_lifecycle', trade_lifecycle_type='sent', data=message)
        except BinanceAPIException as e:
            logger().error('Failed to create order. Exception was: {}'.format(e))

    def cancel_order(self, base, quote, internal_order_id, request_id, requester_id=None, exchange_order_id=None):
        try:
            self.client.cancel_order(symbol=base+quote, origClientOrderId=internal_order_id)
            message = dict()
            message['action'] = 'cancel_sent'
            message['internal_order_id'] = internal_order_id
            message['request_id'] = request_id
            message['exchange'] = self.name
            self.user_data_service.pending_cancel[internal_order_id] = True
            self.notify_callbacks('trade_lifecycle', trade_lifecycle_type='cancel_sent', data=message)
        except BinanceAPIException as e:
            logger().error('Failed to cancel order. Exception was: {}'.format(e))
            if str(e.code) == '-2011':
                self.notify_callbacks('trade_lifecycle', data={
                    'action': 'CANCEL_FAILED',
                    'reason': 'order_not_found',
                    'base': base,
                    'quote': quote,
                    'exchange': self.name,
                    'exchange_order_id': exchange_order_id,
                    'internal_order_id': internal_order_id,
                    'order_status': 'UNKNOWN',
                    'server_ms': int(round(time.time() * 1000)),
                    'received_ms': int(round(time.time() * 1000))
                })


    def get_balances(self):
        try:
            account = self.client.get_account()
            self.notify_callbacks('account', account_type='balance', data=account['balances'])
            return account['balances']
        except BinanceAPIException as e:
            logger().error('Failed to get balances.  Exception was: {}'.format(e))

    def get_deposits(self, currency=None):
        if currency is not None:
            history = self.client.get_deposit_history(asset=currency)
        else:
            history = self.client.get_deposit_history()

        deposits = history['depositList']
        formatted_deposits = []

        for deposit in deposits:
            formatted_deposits.append({
                'time': deposit['insertTime'] / 1000,
                'asset': deposit['asset'],
                'amount': deposit['amount'],
                'status': 'pending' if deposit['status'] == 0 else 'complete'
            })

        return formatted_deposits

    def get_withdrawals(self, currency=None):
        if currency is not None:
            history = self.client.get_withdraw_history(asset=currency)
        else:
            history = self.client.get_withdraw_history()

        withdrawals = history['withdrawalList']
        formatted_withdrawal = []

        for withdrawal in withdrawals:
            formatted_withdrawal.append({
                'time': withdrawal['insertTime'] / 1000,
                'asset': withdrawal['asset'],
                'amount': withdrawal['amount'],
                'status': 'complete' if withdrawal['status'] == 6 else 'pending',
                'to_address': withdrawal['address']
            })

        return formatted_withdrawal

    def can_withdraw(self):
        return True

    def withdraw(self, currency, amount, address, tag=None, cb=None, **kwargs):
        if tag is not None:
            response = self.client.withdraw(asset=currency, address=address, amount=amount)
        else:
            response = self.client.withdraw(asset=currency, address=address, addressTag=tag, amount=amount)

        return response['success']

    def get_deposit_address(self, currency):
        response = self.client.get_deposit_address(asset=currency)
        return {
            'address': response['address'],
            'tag': response['addressTag']
        }

    @staticmethod
    def get_limits():
        from requests import get
        url = 'https://api.binance.com/api/v1/exchangeInfo'
        r = get(url)
        j = r.json()
        return j
