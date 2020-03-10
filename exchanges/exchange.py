# This interface definition should go here
from _decimal import Decimal
from abc import abstractmethod

from aj_sns.transfer_service import TransferService
from pandas import DataFrame, concat, to_numeric


class Exchange(TransferService):

    def __init__(self, name):
        super().__init__()
        self.callbacks = {}
        self.name = name

    def notify_callbacks(self, topic, **data):
        for f in self.callbacks.values():
            f(topic, **data)

    def add_callback(self, name, callback):
        self.callbacks[name] = callback

    def remove_callback(self, name):
        del self.callbacks[name]

    def remove_all_callbacks(self):
        self.callbacks = {}

    def can_withdraw(self, currency):
        return False

    def can_deposit(self, currency):
        return False

    @abstractmethod
    def create_order(self, base, quote, price, quantity, side, order_type, internal_order_id, request_id=None,
                     requester_id=None, **kwargs):
        pass

    @abstractmethod
    def cancel_order(self, base, quote, internal_order_id, request_id, requester_id=None, exchange_order_id=None):
        pass

    @abstractmethod
    def cancel_all(self, base, quote):
        pass

    @abstractmethod
    def get_order_book(self, base, quote):
        pass

    @abstractmethod
    def get_balances(self):
        pass

    @abstractmethod
    def get_deposit_address(self, currency):
        pass

    @abstractmethod
    def withdraw(self, currency, amount, address, tag=None, cb=None, **kwargs):
        pass

    @abstractmethod
    def get_deposits(self, currency=None):
        pass

    @abstractmethod
    def follow_market(self, base, quote):
        pass

    # unfollow_user_data and unfollow_order_book
    @abstractmethod
    def unfollow_market(self, base, quote):
        pass

    @abstractmethod
    def unfollow_all(self):
        pass

    @abstractmethod
    def get_public_trades(self, base, quote, start_s, end_s):
            pass

    @abstractmethod
    def get_our_trades(self, base, quote, start_s, end_s):
        pass
