# coding=utf-8

import binascii
import codecs
import re
import traceback
import sys
import threading
import uuid
from collections import OrderedDict

import ethereum
import requests
import time

from decimal import Decimal

from aj_sns.creds_retriever import get_creds
from aj_sns.transfer_service import TransferService
from ethereum.utils import sha3, ecsign, encode_int32
from aj_sns.log_service import logger
from pandas import DataFrame, concat, to_numeric

from exchanges.idex.exceptions import IdexException, IdexWalletAddressNotFoundException, IdexPrivateKeyNotFoundException, IdexAPIException, IdexRequestException, IdexCurrencyNotFoundException
from exchanges.common.open_order_tracker import OrderTracker


class IdexService(OrderTracker, TransferService):

    API_URL = 'https://api.idex.market'
    #API_URL = 'https://api-regional.idex.market'

    _wallet_address = None
    _private_key = None
    _contract_address = None
    _currency_addresses = {}

    def __init__(self, name, address=None, private_key=None, poll_time_s=5.0, tick_tock=True):
        OrderTracker.__init__(self)
        self.name = name

        self._start_nonce = None
        self._client_started = int(time.time() * 1000)

        self.session = self._init_session()

        if address:
            self.set_wallet_address(address, private_key)

        self.ws_connected = False
        self.books = {}
        self.markets_following = {}
        self.trades_following = {}
        self.following = {}
        self.websocket = None
        self.callbacks = {}
        self.poll_time_s = poll_time_s
        self.name = name

        if tick_tock is True:
            threading.Timer(self.poll_time_s, self.on_tick).start()

    def _init_session(self):

        session = requests.session()
        headers = {'Accept': 'application/json',
                   'User-Agent': 'python-idex'}
        session.headers.update(headers)
        return session

    def _get_nonce(self):
        """Get a unique nonce for request

        """
        return self._start_nonce + int(time.time() * 1000) - self._client_started

    def _generate_signature(self, data):
        """Generate v, r, s values from payload

        """

        # pack parameters based on type
        sig_str = b''
        for d in data:
            val = d[1]
            if d[2] == 'address':
                # remove 0x prefix and convert to bytes
                val = val[2:].encode('utf-8')
            elif d[2] == 'uint256':
                # encode, pad and convert to bytes
                val = binascii.b2a_hex(encode_int32(int(d[1])))
            sig_str += val

        # hash the packed string
        rawhash = sha3(codecs.decode(sig_str, 'hex_codec'))

        # salt the hashed packed string
        salted = sha3(u"\x19Ethereum Signed Message:\n32".encode('utf-8') + rawhash)

        # sign string
        v, r, s = ecsign(salted, codecs.decode(self._private_key[2:], 'hex_codec'))

        # pad r and s with 0 to 64 places
        return {'v': v, 'r': "{0:#0{1}x}".format(r, 66), 's': "{0:#0{1}x}".format(s, 66)}

    def _create_uri(self, path, base_url):
        return '{}/{}'.format(base_url, path)

    def _request(self, method, path, signed, base_url, **kwargs):

        kwargs['json'] = kwargs.get('json', {})
        kwargs['headers'] = kwargs.get('headers', {})

        uri = self._create_uri(path, base_url)

        if signed:
            # generate signature e.g. {'v': 28 (or 27), 'r': '0x...', 's': '0x...'}
            kwargs['json'].update(self._generate_signature(kwargs['hash_data']))

            # put hash_data into json param
            for name, value, _param_type in kwargs['hash_data']:
                kwargs['json'][name] = value

            # filter out contract address, not required
            if 'contract_address' in kwargs['json']:
                del(kwargs['json']['contract_address'])

            # remove the passed hash data
            del(kwargs['hash_data'])

        f = getattr(self.session, method)
        response = f(uri, **kwargs, timeout=15)
        return self._handle_response(response)

    def _handle_response(self, response):
        """Internal helper for handling API responses from the Quoine server.
        Raises the appropriate exceptions when necessary; otherwise, returns the
        response.
        """
        if not str(response.status_code).startswith('2'):
            raise IdexAPIException(response)
        try:
            res = response.json()
            if 'error' in res:
                raise IdexAPIException(response)
            return res
        except ValueError:
            raise IdexRequestException('Invalid Response: %s' % response.text)

    def _get(self, path, signed=False, **kwargs):
        return self._request('get', path, signed, **kwargs)

    def _post(self, path, signed=False, base_url=API_URL, **kwargs):
        return self._request('post', path, signed, base_url, **kwargs)

    def _put(self, path, signed=False, **kwargs):
        return self._request('put', path, signed, **kwargs)

    def _delete(self, path, signed=False, **kwargs):
        return self._request('delete', path, signed, **kwargs)

    def set_wallet_address(self, address, private_key=None):
        """Set the wallet address. Optionally add the private_key, this is only required for trading.

        :param address: Address of the wallet to use
        :type address: address string
        :param private_key: optional - The private key for the address
        :type private_key: string

        .. code:: python

            client.set_wallet_address('0x925cfc20de3fcbdba2d6e7c75dbb1d0a3f93b8a3', 'priv_key...')

        :returns: nothing

        """
        self._wallet_address = address.lower()
        nonce_res = self.get_my_next_nonce()
        if hasattr(nonce_res, 'nonce'):
            self._start_nonce = nonce_res['nonce']
        else:
            time_ms = int(time.time() * 1000)
            logger().error('Failed to get nonce. Falling back to time: ' + str(time_ms))
            self._start_nonce = time_ms
        if private_key:
            if re.match(r"^0x[0-9a-zA-Z]{64}$", private_key) is None:
                raise(IdexException("Private key in invalid format must satisfy 0x[0-9a-zA-Z]{64}"))
            self._private_key = private_key

    def get_wallet_address(self):
        """Get the wallet address

        .. code:: python

            address = client.get_wallet_address()

        :returns: address string

        """
        return self._wallet_address

    # Market Endpoints

    def get_tickers(self):
        """Get all market tickers

        Please note: If any field is unavailable due to a lack of trade history or a lack of 24hr data, the field will be set to 'N/A'. percentChange, baseVolume, and quoteVolume will never be 'N/A' but may be 0.

        https://github.com/AuroraDAO/idex-api-docs#returnticker

        .. code:: python

            tickers = client.get_tickers()

        :returns: API Response

        .. code-block:: python

            {
                ETH_SAN:  {
                    last: '0.000981',
                    high: '0.0010763',
                    low: '0.0009777',
                    lowestAsk: '0.00098151',
                    highestBid: '0.0007853',
                    percentChange: '-1.83619353',
                    baseVolume: '7.3922603247161',
                    quoteVolume: '7462.998433'
                },
                ETH_LINK: {
                    last: '0.001',
                    high: '0.0014',
                    low: '0.001',
                    lowestAsk: '0.002',
                    highestBid: '0.001',
                    percentChange: '-28.57142857',
                    baseVolume: '13.651606265667369466',
                    quoteVolume: '9765.891979953083752189'
                }
                # all possible markets follow ...
            }

        :raises:  IdexResponseException,  IdexAPIException

        """

        return self._post('returnTicker')

    def get_ticker(self, market):
        """Get ticker for selected market

        Please note: If any field is unavailable due to a lack of trade history or a lack of 24hr data, the field will be set to 'N/A'. percentChange, baseVolume, and quoteVolume will never be 'N/A' but may be 0.

        https://github.com/AuroraDAO/idex-api-docs#returnticker

        :param market: Name of market e.g. ETH_SAN
        :type market: string

        .. code:: python

            ticker = client.get_ticker('ETH_SAN')

        :returns: API Response

        .. code-block:: python

            {
                last: '0.000981',
                high: '0.0010763',
                low: '0.0009777',
                lowestAsk: '0.00098151',
                highestBid: '0.0007853',
                percentChange: '-1.83619353',
                baseVolume: '7.3922603247161',
                quoteVolume: '7462.998433'
            }

        :raises:  IdexResponseException,  IdexAPIException

        """

        data = {
            'market': market
        }

        return self._post('returnTicker', False, json=data)

    def get_24hr_volume(self):

        """Get all market tickers

        https://github.com/AuroraDAO/idex-api-docs#return24volume

        .. code:: python

            volume = client.get_24hr_volume()

        :returns: API Response

        .. code-block:: python

            {
                ETH_REP: {
                    ETH: '1.3429046745',
                    REP: '105.29046745'
                },
                ETH_DVIP: {
                    ETH: '4',
                    DVIP: '4'
                },
                totalETH: '5.3429046745'
            }

        :raises:  IdexResponseException,  IdexAPIException

        """

        return self._post('return24Volume')

    def get_order_books(self):
        """Get an object of the entire order book keyed by market

        Each market returned will have an asks and bids property containing all the sell orders and buy orders sorted by best price. Order objects will contain a price amount total and orderHash property but also a params property which will contain additional data about the order useful for filling or verifying it.

        https://github.com/AuroraDAO/idex-api-docs#returnorderbook

        .. code:: python

            orderbooks = client.get_order_books()

        :returns: API Response

        .. code-block:: python

            {
                ETH_DVIP: {
                    asks: [
                        {
                            price: '2',
                            amount: '1',
                            total: '2',
                            orderHash: '0x6aee6591def621a435dd86eafa32dfc534d4baa38d715988d6f23f3e2f20a29a',
                            params: {
                                tokenBuy: '0x0000000000000000000000000000000000000000',
                                buySymbol: 'ETH',
                                buyPrecision: 18,
                                amountBuy: '2000000000000000000',
                                tokenSell: '0xf59fad2879fb8380ffa6049a48abf9c9959b3b5c',
                                sellSymbol: 'DVIP',
                                sellPrecision: 8,
                                amountSell: '100000000',
                                expires: 190000,
                                nonce: 164,
                                user: '0xca82b7b95604f70b3ff5c6ede797a28b11b47d63'
                            }
                        }
                    ],
                    bids: [
                        {
                            price: '1',
                            amount: '2',
                            total: '2',
                            orderHash: '0x9ba97cfc6d8e0f9a72e9d26c377be6632f79eaf4d87ac52a2b3d715003b6536e',
                            params: {
                                tokenBuy: '0xf59fad2879fb8380ffa6049a48abf9c9959b3b5c',
                                buySymbol: 'DVIP',
                                buyPrecision: 8,
                                amountBuy: '200000000',
                                tokenSell: '0x0000000000000000000000000000000000000000',
                                sellSymbol: 'ETH',
                                sellPrecision: 18,
                                amountSell: '2000000000000000000',
                                expires: 190000,
                                nonce: 151,
                                user: '0xca82b7b95604f70b3ff5c6ede797a28b11b47d63'
                            }
                        }
                    ]
                }
            }

        :raises:  IdexResponseException,  IdexAPIException

        """

        return self._post('returnOrderBook')

    def get_order_book(self, base, quote):
        """Get order book for selected market

        Each market returned will have an asks and bids property containing all the sell orders and buy orders sorted by best price. Order objects will contain a price amount total and orderHash property but also a params property which will contain additional data about the order useful for filling or verifying it.

        https://github.com/AuroraDAO/idex-api-docs#returnorderbook

        :param market: Name of market e.g. ETH_SAN
        :type market: string

        .. code:: python

            orderbook = client.get_order_book('ETH_SAN')

        :returns: API Response

        .. code-block:: python

            {
                asks: [
                    {
                        price: '2',
                        amount: '1',
                        total: '2',
                        orderHash: '0x6aee6591def621a435dd86eafa32dfc534d4baa38d715988d6f23f3e2f20a29a',
                        params: {
                            tokenBuy: '0x0000000000000000000000000000000000000000',
                            buySymbol: 'ETH',
                            buyPrecision: 18,
                            amountBuy: '2000000000000000000',
                            tokenSell: '0xf59fad2879fb8380ffa6049a48abf9c9959b3b5c',
                            sellSymbol: 'DVIP',
                            sellPrecision: 8,
                            amountSell: '100000000',
                            expires: 190000,
                            nonce: 164,
                            user: '0xca82b7b95604f70b3ff5c6ede797a28b11b47d63'
                        }
                    }
                ],
                bids: [
                    {
                        price: '1',
                        amount: '2',
                        total: '2',
                        orderHash: '0x9ba97cfc6d8e0f9a72e9d26c377be6632f79eaf4d87ac52a2b3d715003b6536e',
                        params: {
                            tokenBuy: '0xf59fad2879fb8380ffa6049a48abf9c9959b3b5c',
                            buySymbol: 'DVIP',
                            buyPrecision: 8,
                            amountBuy: '200000000',
                            tokenSell: '0x0000000000000000000000000000000000000000',
                            sellSymbol: 'ETH',
                            sellPrecision: 18,
                            amountSell: '2000000000000000000',
                            expires: 190000,
                            nonce: 151,
                            user: '0xca82b7b95604f70b3ff5c6ede797a28b11b47d63'
                        }
                    }
                ]
            }

        :raises:  IdexResponseException,  IdexAPIException

        """
        market = self.to_market(base, quote)

        data = {
            'market': market
        }

        book = self._post('returnOrderBook', False, json=data, base_url='https://api-regional.idex.market')
        #response = getattr(self.session, 'get')('https://api-regional.idex.market/returnOrderBook?market=' + market)
        #book = self._handle_response(response)

        book_by_hash = {'bids': OrderedDict(), 'asks': OrderedDict()}
        for bid in book['bids']:
            book_by_hash['bids'][bid['orderHash']] = bid
        for ask in book['asks']:
            book_by_hash['asks'][ask['orderHash']] = ask

        return book_by_hash

    def to_market(self, base, quote):
        return quote.upper() + '_' + base.upper()

    def to_base_and_quote(self, market):
        parts = market.split('_')
        quote = parts[0]
        base = parts[1]
        return base, quote

    def on_tick(self):
        logger().info('tick')
        try:
            for market in self.markets_following.keys():
                logger().info('Getting market: ' + market)
                base, quote = self.to_base_and_quote(market)
                book = self.get_order_book(base, quote)
                open_orders_to_del = []
                internal_order_to_del_from_map = []
                trade_lifecycle_actions = []
                for open_order in self.open_orders.values():
                    if open_order['base'] == base and open_order['quote'] == quote:
                        side = 'bids' if open_order['side'] == 'buy' else 'asks'

                        if open_order['exchange_order_id'] in book[side]:
                            book_match = book[side][open_order['exchange_order_id']]
                            expected_quantity_on_book = open_order['quantity'] - open_order['cum_quantity_filled']
                            actual_quantity_on_book = Decimal(book_match['amount'])
                            new_fill_amount = expected_quantity_on_book - actual_quantity_on_book
                            status = 'PARTIALLY_FILLED'
                            open_order['cum_quantity_filled'] = open_order['cum_quantity_filled'] + new_fill_amount
                        else:
                            # If it's not in the book, and it's open on our end, it must have been fully filled
                            # If it's a pending cancel, ignore it for now. It'll either be canceled thus not in the book
                            # Or fail to cancel and be picked up next time as a full execution
                            if open_order['internal_order_id'] not in self.pending_cancel:
                                new_fill_amount = open_order['quantity'] - open_order['cum_quantity_filled']
                                status = 'FILLED'
                                open_order['cum_quantity_filled'] = open_order['quantity']
                                # Since it's fully filled, let's remove it from our open_orders
                                open_orders_to_del.append(open_order['exchange_order_id'])
                                internal_order_to_del_from_map.append(open_order['internal_order_id'])
                            else:
                                # Order is missing because it's being/been canceled. In the off chance the cancel fails
                                # Due to a full fill happening, the fill will get picked up in the next tick
                                new_fill_amount = 0

                        if new_fill_amount > 0:

                            if open_order['side'] == 'buy':
                                fee_base = Decimal('0.1') * new_fill_amount
                                fee_quote = Decimal('0')
                            else:
                                fee_base = Decimal('0')
                                fee_quote = Decimal('0.1') * new_fill_amount * open_order['price']

                            message = {
                                'action': 'EXECUTION',
                                'exchange': self.name,
                                'base': base,
                                'quote': quote,
                                'exchange_order_id': open_order['exchange_order_id'],
                                'internal_order_id': open_order['internal_order_id'],
                                'side': open_order['side'],
                                'quantity': open_order['quantity'],
                                'price': open_order['price'],
                                'cum_quantity_filled': open_order['cum_quantity_filled'],
                                'order_status': status,
                                'server_ms': int(round(time.time() * 1000)),
                                'received_ms': int(round(time.time() * 1000)),
                                'last_executed_quantity': new_fill_amount,
                                'last_executed_price': open_order['price'],
                                'fee_base': fee_base,
                                'fee_quote': fee_quote,
                                'trade_id': '-1'
                            }
                            trade_lifecycle_actions.append(message)
                for order_id in open_orders_to_del:
                    self.open_orders.pop(order_id, None)
                for internal_id in internal_order_to_del_from_map:
                    self.internal_to_external_id.pop(internal_id, None)
                for action in trade_lifecycle_actions:
                    self.notify_callbacks('trade_lifecycle', trade_lifecycle_type=message['action'], data=action)

                # Aggregate the book
                aggregated_book = {'bids': OrderedDict(), 'asks': OrderedDict()}
                for bid in book['bids'].values():
                    if bid['params']['user'] == self._wallet_address:
                        # Cancel this order as it's ours but we're not aware of it
                        self.cancel_order(base, quote, bid['orderHash'], str(uuid.uuid4()), retries=5,
                                          exchange_order_id=bid['orderHash'], cb=False)
                    else:
                        if bid['price'] in aggregated_book['bids']:
                            aggregated_book['bids'][bid['price']] += Decimal(bid['amount'])
                        else:
                            aggregated_book['bids'][bid['price']] = Decimal(bid['amount'])
                for ask in book['asks'].values():
                    if ask['params']['user'] == self._wallet_address:
                        # Cancel this order as it's ours but we're not aware of it
                        self.cancel_order(base, quote, ask['orderHash'], str(uuid.uuid4()), retries=5,
                                          exchange_order_id=ask['orderHash'], cb=False)
                    else:
                        if ask['price'] in aggregated_book['asks']:
                            aggregated_book['asks'][ask['price']] += Decimal(ask['amount'])
                        else:
                            aggregated_book['asks'][ask['price']] = Decimal(ask['amount'])

                if len(aggregated_book['bids']) > 0 and len(aggregated_book['asks']) > 0:
                    logger().info('Got book for market ({})'.format(market))

                aggregated_book_as_list = self._as_order_book_list(aggregated_book)
                aggregated_book_as_list['base'] = base
                aggregated_book_as_list['quote'] = quote
                aggregated_book_as_list['exchange'] = self.name

                self.notify_callbacks('order_book', data=aggregated_book_as_list)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            logger().error('on_tick failed with error: ' + str(e))
        finally:
            logger().info('tock')
            threading.Timer(self.poll_time_s, self.on_tick).start()

    @staticmethod
    def _as_order_book_list(book):
        sorted_book = {'bids': [], 'asks': []}

        for k in book['bids']:
            sorted_book['bids'].append([k, book['bids'][k]])

        for k in book['asks']:
            sorted_book['asks'].append([k, book['asks'][k]])

        return sorted_book

    def get_open_orders(self, market, address):
        """Get the open orders for a given market and address

        Output is similar to the output for get_order_book() except that orders are not sorted by type or price, but are rather displayed in the order of insertion. As is the case with get_order_book( there is a params property of the response value that contains details on the order which can help with verifying its authenticity.

        https://github.com/AuroraDAO/idex-api-docs#returnopenorders

        :param market: Name of market e.g. ETH_SAN
        :type market: string
        :param address: Address to return open orders associated with
        :type address: address string

        .. code:: python

            orders = client.get_open_orders(
                'ETH_SAN',
                '0xca82b7b95604f70b3ff5c6ede797a28b11b47d63')

        :returns: API Response

        .. code-block:: python

            [
                {
                    orderNumber: 1412,
                    orderHash: '0xf1bbc500af8d411b0096ac62bc9b60e97024ad8b9ea170340ff0ecfa03536417',
                    price: '2.3',
                    amount: '1.2',
                    total: '2.76',
                    type: 'sell',
                    params: {
                        tokenBuy: '0x0000000000000000000000000000000000000000',
                        buySymbol: 'ETH',
                        buyPrecision: 18,
                        amountBuy: '2760000000000000000',
                        tokenSell: '0xf59fad2879fb8380ffa6049a48abf9c9959b3b5c',
                        sellSymbol: 'DVIP',
                        sellPrecision: 8,
                        amountSell: '120000000',
                        expires: 190000,
                        nonce: 166,
                        user: '0xca82b7b95604f70b3ff5c6ede797a28b11b47d63'
                    }
                },
                {
                    orderNumber: 1413,
                    orderHash: '0x62748b55e1106f3f453d51f9b95282593ef5ce03c22f3235536cf63a1476d5e4',
                    price: '2.98',
                    amount: '1.2',
                    total: '3.576',
                    type: 'sell',
                    params:{
                        tokenBuy: '0x0000000000000000000000000000000000000000',
                        buySymbol: 'ETH',
                        buyPrecision: 18,
                        amountBuy: '3576000000000000000',
                        tokenSell: '0xf59fad2879fb8380ffa6049a48abf9c9959b3b5c',
                        sellSymbol: 'DVIP',
                        sellPrecision: 8,
                        amountSell: '120000000',
                        expires: 190000,
                        nonce: 168,
                        user: '0xca82b7b95604f70b3ff5c6ede797a28b11b47d63'
                    }
                }
            ]

        :raises:  IdexResponseException,  IdexAPIException

        """

        data = {
            'market': market,
            'address': address
        }

        return self._post('returnOpenOrders', False, json=data)

    def get_my_open_orders(self, market):
        """Get your open orders for a given market

        Output is similar to the output for get_order_book() except that orders are not sorted by type or price, but are rather displayed in the order of insertion. As is the case with get_order_book( there is a params property of the response value that contains details on the order which can help with verifying its authenticity.

        https://github.com/AuroraDAO/idex-api-docs#returnopenorders

        :param market: Name of market e.g. ETH_SAN
        :type market: string

        .. code:: python

            orders = client.get_my_open_orders('ETH_SAN')

        :returns: API Response

        .. code-block:: python

            [
                {
                    orderNumber: 1412,
                    orderHash: '0xf1bbc500af8d411b0096ac62bc9b60e97024ad8b9ea170340ff0ecfa03536417',
                    price: '2.3',
                    amount: '1.2',
                    total: '2.76',
                    type: 'sell',
                    params: {
                        tokenBuy: '0x0000000000000000000000000000000000000000',
                        buySymbol: 'ETH',
                        buyPrecision: 18,
                        amountBuy: '2760000000000000000',
                        tokenSell: '0xf59fad2879fb8380ffa6049a48abf9c9959b3b5c',
                        sellSymbol: 'DVIP',
                        sellPrecision: 8,
                        amountSell: '120000000',
                        expires: 190000,
                        nonce: 166,
                        user: '0xca82b7b95604f70b3ff5c6ede797a28b11b47d63'
                    }
                },
                {
                    orderNumber: 1413,
                    orderHash: '0x62748b55e1106f3f453d51f9b95282593ef5ce03c22f3235536cf63a1476d5e4',
                    price: '2.98',
                    amount: '1.2',
                    total: '3.576',
                    type: 'sell',
                    params:{
                        tokenBuy: '0x0000000000000000000000000000000000000000',
                        buySymbol: 'ETH',
                        buyPrecision: 18,
                        amountBuy: '3576000000000000000',
                        tokenSell: '0xf59fad2879fb8380ffa6049a48abf9c9959b3b5c',
                        sellSymbol: 'DVIP',
                        sellPrecision: 8,
                        amountSell: '120000000',
                        expires: 190000,
                        nonce: 168,
                        user: '0xca82b7b95604f70b3ff5c6ede797a28b11b47d63'
                    }
                }
            ]

        :raises:  IdexWalletAddressNotFoundException, IdexResponseException,  IdexAPIException

        """

        if not self._wallet_address:
            raise IdexWalletAddressNotFoundException()

        return self.get_open_orders(market, self._wallet_address)

    def get_trade_history(self, market=None, address=None, start=None, end=None):
        """Get the past 200 trades for a given market and address, or up to 10000 trades between a range specified in UNIX timetsamps by the "start" and "end" properties of your JSON input.

        https://github.com/AuroraDAO/idex-api-docs#returntradehistory

        :param market: optional - will return an array of trade objects for the market, if omitted, will return an object of arrays of trade objects keyed by each market
        :type market: string
        :param address: optional - If specified, return value will only include trades that involve the address as the maker or taker.
        :type address: address string
        :param start: optional - The inclusive UNIX timestamp (seconds since epoch) marking the earliest trade that will be returned in the response, (Default - 0)
        :type start: int
        :param end: optional - The inclusive UNIX timestamp marking the latest trade that will be returned in the response. (Default - current timestamp)
        :type end: int

        .. code:: python

            trades = client.get_trade_history()

            # get trades for the last 2 hours for ETH EOS market
            start = int(time.time()) - (60 * 2) # 2 hours ago
            trades = client.get_trade_history(market='ETH_EOS', start=start)

        :returns: API Response

        .. code-block:: python

            {
                ETH_REP: [
                    {
                        date: '2017-10-11 21:41:15',
                        amount: '0.3',
                        type: 'buy',
                        total: '1',
                        price: '0.3',
                        orderHash: '0x600c405c44d30086771ac0bd9b455de08813127ff0c56017202c95df190169ae',
                        uuid: 'e8719a10-aecc-11e7-9535-3b8451fd4699',
                        transactionHash: '0x28b945b586a5929c69337929533e04794d488c2d6e1122b7b915705d0dff8bb6'
                    }
                ]
            }

        :raises:  IdexResponseException,  IdexAPIException

        """

        data = {}
        if market:
            data['market'] = market
        if address:
            data['address'] = address
        if start:
            data['start'] = start
        if end:
            data['end'] = end

        return self._post('returnTradeHistory', False, json=data)

    def get_my_trade_history(self, market=None, start=None, end=None):
        """Get your past 200 trades for a given market, or up to 10000 trades between a range specified in UNIX timetsamps by the "start" and "end" properties of your JSON input.

        https://github.com/AuroraDAO/idex-api-docs#returntradehistory

        :param market: optional - will return an array of trade objects for the market, if omitted, will return an object of arrays of trade objects keyed by each market
        :type market: string
        :param address: optional - If specified, return value will only include trades that involve the address as the maker or taker.
        :type address: address string
        :param start: optional - The inclusive UNIX timestamp (seconds since epoch) marking the earliest trade that will be returned in the response, (Default - 0)
        :type start: int
        :param end: optional - The inclusive UNIX timestamp marking the latest trade that will be returned in the response. (Default - current timestamp)
        :type end: int

        .. code:: python

            trades = client.get_my_trade_history()

            # get trades for the last 2 hours for ETH EOS market
            start = int(time.time()) - (60 * 2) # 2 hours ago
            trades = client.get_my_trade_history(market='ETH_EOS', start=start)

        :returns: API Response

        .. code-block:: python

            {
                ETH_REP: [
                    {
                        date: '2017-10-11 21:41:15',
                        amount: '0.3',
                        type: 'buy',
                        total: '1',
                        price: '0.3',
                        orderHash: '0x600c405c44d30086771ac0bd9b455de08813127ff0c56017202c95df190169ae',
                        uuid: 'e8719a10-aecc-11e7-9535-3b8451fd4699',
                        transactionHash: '0x28b945b586a5929c69337929533e04794d488c2d6e1122b7b915705d0dff8bb6'
                    }
                ]
            }

        :raises:  IdexWalletAddressNotFoundException, IdexResponseException,  IdexAPIException

        """

        if not self._wallet_address:
            raise IdexWalletAddressNotFoundException()

        return self.get_trade_history(market, self._wallet_address, start, end)

    def get_currencies(self):
        """Get token data indexed by symbol

        https://github.com/AuroraDAO/idex-api-docs#returncurrencies

        .. code:: python

            currencies = client.get_currencies()

        :returns: API Response

        .. code-block:: python

            {
                ETH: {
                    decimals: 18,
                    address: '0x0000000000000000000000000000000000000000',
                    name: 'Ether'
                },
                REP: {
                    decimals: 8,
                    address: '0xc853ba17650d32daba343294998ea4e33e7a48b9',
                    name: 'Reputation'
                },
                DVIP: {
                    decimals: 8,
                    address: '0xf59fad2879fb8380ffa6049a48abf9c9959b3b5c',
                    name: 'Aurora'
                }
            }

        :raises:  IdexResponseException,  IdexAPIException

        """

        return self._post('returnCurrencies')

    def get_currency(self, currency):
        """Get the details for a particular currency using it's token name or address

        :param currency: Name of the currency e.g. EOS or '0x7c5a0ce9267ed19b22f8cae653f198e3e8daf098'
        :type currency: string or hex string

        .. code:: python

            # using token name
            currency = client.get_currency('REP')

            # using the address string
            currency = client.get_currency('0xc853ba17650d32daba343294998ea4e33e7a48b9')

        :returns:

        .. code-block:: python

            {
                decimals: 8,
                address: '0xc853ba17650d32daba343294998ea4e33e7a48b9',
                name: 'Reputation'
            }

        :raises:  IdexCurrencyNotFoundException, IdexResponseException,  IdexAPIException

        """

        if currency not in self._currency_addresses:
            self._currency_addresses = self.get_currencies()

        res = None
        if currency[:2] == '0x':
            for token, c in self._currency_addresses.items():
                if c['address'] == currency:
                    res = c
                    break
            # check if we found the currency
            if res is None:
                raise IdexCurrencyNotFoundException(currency)
        else:
            if currency not in self._currency_addresses:
                raise IdexCurrencyNotFoundException(currency)
            res = self._currency_addresses[currency]

        return res

    def get_balances(self):
        return self.get_my_balances(complete=True)

    def get_balances_internal(self, address=None, complete=True, retries=0):
        """Get available balances for an address (total deposited minus amount in open orders) indexed by token symbol.

        https://github.com/AuroraDAO/idex-api-docs#returnbalances

        :param address: Address to query balances of
        :type address: address string
        :param complete: Include available balances along with the amount you have in open orders for each token (Default False)
        :param complete: bool

        .. code:: python

            balances = client.get_balances('0xca82b7b95604f70b3ff5c6ede797a28b11b47d63')

        :returns: API Response

        .. code-block:: python

            # Without complete details
            {
                REP: '25.55306545',
                DVIP: '200000000.31012358'
            }

            # With complete details
            {
                REP: {
                    available: '25.55306545',
                    onOrders: '0'
                },
                DVIP: {
                    available: '200000000.31012358',
                    onOrders: '0'
                }
            }

        :raises:  IdexResponseException,  IdexAPIException

        """

        if address is None:
            address = self._wallet_address

        if not address:
            raise IdexWalletAddressNotFoundException()

        data = {
            'address': address
        }

        path = 'returnBalances'
        if complete:
            path = 'returnCompleteBalances'

        success = False

        while retries >= 0 and not success:
            try:
                response = self._post(path, False, json=data)
                success = True
            except Exception as e:
                logger().error('Failed to get balances with error: ' + str(e))
            finally:
                retries = retries - 1
                time.sleep(4)

        internal_format = []

        if success:
            for asset in response:
                if 'available' in response[asset]:
                    internal_format.append({
                        'asset': asset,
                        'free': Decimal(response[asset]['available']),
                        'locked': Decimal(response[asset]['onOrders'])})
                else:
                    internal_format.append({
                        'asset': asset,
                        'free': Decimal(response[asset]),
                        'locked': Decimal('0')})
            self.notify_callbacks('account', account_type='balance', data=internal_format)
        else:
            self.notify_callbacks('account', account_type='balances_failed', data={})

        return internal_format

    def get_my_balances(self, complete=True):
        """Get your available balances (total deposited minus amount in open orders) indexed by token symbol.

        https://github.com/AuroraDAO/idex-api-docs#returnbalances

        :param complete: Include available balances along with the amount you have in open orders for each token (Default False)
        :param complete: bool

        .. code:: python

            balances = client.get_my_balances()

        :returns: API Response

        .. code-block:: python

            # Without complete details
            {
                REP: '25.55306545',
                DVIP: '200000000.31012358'
            }

            # With complete details
            {
                REP: {
                    available: '25.55306545',
                    onOrders: '0'
                },
                DVIP: {
                    available: '200000000.31012358',
                    onOrders: '0'
                }
            }

        :raises:  IdexWalletAddressNotFoundException, IdexResponseException,  IdexAPIException

        """

        if not self._wallet_address:
            raise IdexWalletAddressNotFoundException()

        return self.get_balances_internal(self._wallet_address, complete)

    def get_transfers(self, address, start=None, end=None):
        """Returns the deposit and withdrawal history for an address within a range, specified by the "start" and "end" properties of the JSON input, both of which must be UNIX timestamps. Withdrawals can be marked as "PENDING" if they are queued for dispatch, "PROCESSING" if the transaction has been dispatched, and "COMPLETE" if the transaction has been mined.

        https://github.com/AuroraDAO/idex-api-docs#returndepositswithdrawals

        :param address: Address to query deposit/withdrawal history for
        :type address: address string
        :param start: optional - Inclusive starting UNIX timestamp of returned results (Default - 0)
        :type start: int
        :param end: optional -  Inclusive ending UNIX timestamp of returned results (Default - current timestamp)
        :type end: int

        .. code:: python

            transfers = client.get_transfers('0xca82b7b95604f70b3ff5c6ede797a28b11b47d63')

        :returns: API Response

        .. code-block:: python

            {
                deposits: [
                    {
                        depositNumber: 265,
                        currency: 'ETH',
                        amount: '4.5',
                        timestamp: 1506550595,
                        transactionHash: '0x52897291dba0a7b255ee7a27a8ca44a9e8d6919ca14f917616444bf974c48897'
                    }
                ],
                withdrawals: [
                    {
                        withdrawalNumber: 174,
                        currency: 'ETH',
                        amount: '4.5',
                        timestamp: 1506552152,
                        transactionHash: '0xe52e9c569fe659556d1e56d8cca2084db0b452cd889f55ec3b4e2f3af61faa57',
                        status: 'COMPLETE'
                    }
                ]
            }

        :raises:  IdexResponseException,  IdexAPIException

        """

        data = {
            'address': address
        }
        if start:
            data['start'] = start
        if end:
            data['end'] = end

        return self._post('returnDepositsWithdrawals', False, json=data)

    def get_my_transfers(self, start=None, end=None):
        """Returns your deposit and withdrawal history within a range, specified by the "start" and "end" properties of the JSON input, both of which must be UNIX timestamps. Withdrawals can be marked as "PENDING" if they are queued for dispatch, "PROCESSING" if the transaction has been dispatched, and "COMPLETE" if the transaction has been mined.

        https://github.com/AuroraDAO/idex-api-docs#returndepositswithdrawals

        :param start: optional - Inclusive starting UNIX timestamp of returned results (Default - 0)
        :type start: int
        :param end: optional -  Inclusive ending UNIX timestamp of returned results (Default - current timestamp)
        :type end: int

        .. code:: python

            transfers = client.get_transfers('0xca82b7b95604f70b3ff5c6ede797a28b11b47d63')

        :returns: API Response

        .. code-block:: python

            {
                deposits: [
                    {
                        depositNumber: 265,
                        currency: 'ETH',
                        amount: '4.5',
                        timestamp: 1506550595,
                        transactionHash: '0x52897291dba0a7b255ee7a27a8ca44a9e8d6919ca14f917616444bf974c48897'
                    }
                ],
                withdrawals: [
                    {
                        withdrawalNumber: 174,
                        currency: 'ETH',
                        amount: '4.5',
                        timestamp: 1506552152,
                        transactionHash: '0xe52e9c569fe659556d1e56d8cca2084db0b452cd889f55ec3b4e2f3af61faa57',
                        status: 'COMPLETE'
                    }
                ]
            }

        :raises:  IdexWalletAddressNotFoundException, IdexResponseException,  IdexAPIException

        """

        if not self._wallet_address:
            raise IdexWalletAddressNotFoundException()

        return self.get_transfers(self._wallet_address, start, end)

    def get_order_trades(self, order_hash):
        """Get all trades involving a given order hash, specified by the order_hash

        https://github.com/AuroraDAO/idex-api-docs#returnordertrades

        :param order_hash: The order hash to query for associated trades
        :type order_hash: 256-bit hex string

        .. code:: python

            trades = client.get_order_trades('0x62748b55e1106f3f453d51f9b95282593ef5ce03c22f3235536cf63a1476d5e4')

        :returns: API Response

        .. code-block:: python

            [
                {
                    date: '2017-10-11 21:41:15',
                    amount: '0.3',
                    type: 'buy',
                    total: '1',
                    price: '0.3',
                    uuid: 'e8719a10-aecc-11e7-9535-3b8451fd4699',
                    transactionHash: '0x28b945b586a5929c69337929533e04794d488c2d6e1122b7b915705d0dff8bb6'
                }
            ]

        :raises:  IdexResponseException,  IdexAPIException

        """

        data = {
            'orderHash': order_hash
        }

        return self._post('returnOrderTrades', False, json=data)

    def get_next_nonce(self, address):
        """Get the lowest nonce that you can use from the given address in one of the trade functions

        https://github.com/AuroraDAO/idex-api-docs#returnnextnonce

        :param address: The address to query for the next nonce to use
        :type address: address string

        .. code:: python

            nonce = client.get_next_nonce('0xf59fad2879fb8380ffa6049a48abf9c9959b3b5c')

        :returns: API Response

        .. code-block:: python

            {
                nonce: 2650
            }

        :raises:  IdexResponseException,  IdexAPIException

        """

        data = {
            'address': address
        }

        retries = 4
        success = False

        while retries >= 0 and not success:
            try:
                nonce = self._post('returnNextNonce', False, json=data)
                success = True
            except Exception as e:
                logger().warn('Failed to get nonce: ' + str(e))
                time.sleep(2)
            finally:
                retries = retries - 1

        if not success:
            logger().fatal('Failed to get idex nonce after all retries. Exiting')

        return nonce

    def get_my_next_nonce(self):
        """Get the lowest nonce that you can use in one of the trade functions

        https://github.com/AuroraDAO/idex-api-docs#returnnextnonce

        .. code:: python

            nonce = client.get_next_nonce('0xf59fad2879fb8380ffa6049a48abf9c9959b3b5c')

        :returns: API Response

        .. code-block:: python

            {
                nonce: 2650
            }

        :raises:  IdexWalletAddressNotFoundException, IdexResponseException,  IdexAPIException

        """

        if not self._wallet_address:
            raise IdexWalletAddressNotFoundException()

        return self.get_next_nonce(self._wallet_address)

    def _get_contract_address(self):
        """Get a cached contract address value

        """
        if not self._contract_address:
            res = self.get_contract_address()
            self._contract_address = res['address']

        return self._contract_address

    def get_contract_address(self):
        """Get the contract address used for depositing, withdrawing, and posting orders

        https://github.com/AuroraDAO/idex-api-docs#returncontractaddress

        .. code:: python

            trades = client.get_contract_address()

        :returns: API Response

        .. code-block:: python

            {
                address: '0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208'
            }

        :raises:  IdexResponseException,  IdexAPIException

        """

        return self._post('returnContractAddress')

    # Trade Endpoints

    def parse_from_currency_quantity(self, currency, quantity):
        """Convert a quantity string to a float

        :param currency: Name of currency e.g EOS
        :type currency: string
        :param quantity: Quantity value as string '3100000000000000000000'
        :type quantity: string

        :returns: decimal

        """

        currency_details = self.get_currency(currency)
        if currency_details is None:
            return None

        f_q = Decimal(quantity)

        if 'decimals' not in currency_details:
            return f_q

        # divide by currency_details['decimals']
        d_str = "1{}".format(("0" * currency_details['decimals']))
        res = f_q / Decimal(d_str)

        return res

    def _num_to_decimal(self, number):
        if type(number) == float:
            number = Decimal(repr(number))
        elif type(number) == int:
            number = Decimal(number)
        elif type(number) == str:
            number = Decimal(number)

        return number

    def convert_to_currency_quantity(self, currency, quantity):
        """Convert a float quantity to the correct decimal places

        :param currency: Name or address of currency e.g EOS or '0x7c5a0ce9267ed19b22f8cae653f198e3e8daf098'
        :type currency: string
        :param quantity: Quantity value 4.234298924 prefer Decimal or string, int or float should work
        :type quantity: Decimal, string, int, float

        """
        currency_details = self.get_currency(currency)
        if currency_details is None:
            return None

        f_q = self._num_to_decimal(quantity)

        if 'decimals' not in currency_details:
            return f_q

        # multiply by currency_details['decimals']
        m_str = "1{}".format(("0" * currency_details['decimals']))
        res = (f_q * Decimal(m_str)).to_integral_exact()

        return str(res)

    def create_order(self, base, quote, price, quantity, side, order_type, internal_order_id, request_id=None,
                     requester_id=None, **kwargs):
        success = False
        reason = None
        retries = 0
        if 'retries' in kwargs.keys():
            retries = kwargs['retries']

        while not success and retries >= 0:
            try:
                if side == 'buy':
                    response = self._create_order(base, quote, price, quantity)
                elif side == 'sell':
                    # Selling 2 BTC for 20,000 USDT (BTC/USDT cross) (price = 10,000, quantity = 2)
                    # Is the same as buying 20,000 USDT for 2 BTC (USDT/BTC cross)
                    # The price in the latter is 1/10,000 BTC per USDT: (1/initial_price)
                    # The quantity in the latter is 20,000 USDT (initial price * initial quantity)
                    response = self._create_order(quote, base, Decimal('1')/price, quantity*price)
                else:
                    logger().fatal('Invalid side, should be either buy or sell (lowercase)')

                success = True
            except Exception as e:
                if not isinstance(e, IdexAPIException):
                    reason = 'Unknown exception type'
                elif hasattr(e, 'message'):
                    if isinstance(e.message, str) and e.message.startswith('Due to rising gas costs on the Ethereum network,'
                                                                           ' value of order must be at least'):
                        retries = -1 # Because it'll never work if the value is too low
                        reason = 'Invalid quantity'
                    elif isinstance(e.message, str) and e.message == 'You have insufficient funds to place this order.':
                        retries = -1
                        reason = 'Insufficient funds'
                    elif isinstance(e.message, str) and e.message == 'Unusual activity detected, please wait up to an ' \
                                                                     'hour for exchange privileges to be reactivated':
                        retries = -1
                        reason = 'Rate limited'
                    else:
                        reason = 'Unknown server error'
                else:
                    reason = 'Unknown server error'

                if retries > 0:
                    logger().warn('Create order attempt failed with reason ({}) and error message: ({}). Retrying'
                                  .format(reason, e.message if hasattr(e, 'message') else str(e)))
                else:
                    logger().error('Create order failed with reason ({}) and error message: ({}).'
                                   .format(reason, e.message if hasattr(e, 'message') else str(e)))
                time.sleep(2)
            finally:
                retries = retries - 1

        if success:
            internal_response = {
                'action': 'CREATED',
                'exchange': self.name,
                'base': base,
                'quote': quote,
                'exchange_order_id': response['orderHash'],
                'internal_order_id': internal_order_id,
                'side': side,
                'quantity': quantity,
                'price': price,
                'cum_quantity_filled': 0,
                'order_status': 'OPEN',
                'server_ms': response['timestamp'] * 1000,
                'received_ms': int(round(time.time() * 1000))
            }

            self.open_orders[internal_response['exchange_order_id']] = internal_response
            self.internal_to_external_id[internal_order_id] = internal_response['exchange_order_id']
        else:
            internal_response = {
                'action': 'CREATE_FAILED',
                'reason': reason,
                'exchange': self.name,
                'base': base,
                'quote': quote,
                'internal_order_id': internal_order_id,
                'side': side,
                'quantity': quantity,
                'price': price,
                'cum_quantity_filled': 0,
                'received_ms': int(round(time.time() * 1000))
            }

        self.notify_callbacks('trade_lifecycle', data=internal_response)

    def _create_order(self, token_buy, token_sell, price, quantity):
        """Create a limit order

        :param token_buy: The name or address of the token you will receive as a result of the trade e.g. ETH or '0x7c5a0ce9267ed19b22f8cae653f198e3e8daf098'
        :type token_buy: string
        :param token_sell:  The name or address of the token you will lose as a result of the trade e.g. EOS or '0x7c5a0ce9267ed19b22f8cae653f198e3e8daf098'
        :type token_sell: string
        :param price:  The price in token_sell you want to purchase the new token for
        :type price: Decimal, string, int or float
        :param quantity: The amount of token_buy you will receive when the order is fully filled
        :type quantity: Decimal, string, int or float

        .. code:: python

            ticker = client.create_order(
                'EOS',
                'ETH',
                '0.000123',
                '31200.324')

        :returns: API Response

        .. code-block:: python

            {
                orderNumber: 2101,
                orderHash: '0x3fe808be7b5df3747e5534056e9ff45ead5b1fcace430d7b4092e5fcd7161e21',
                price: '0.000129032258064516',
                amount: '3100',
                total: '0.4',
                type: 'buy',
                params: {
                    tokenBuy: '0x7c5a0ce9267ed19b22f8cae653f198e3e8daf098',
                    buyPrecision: 18,
                    amountBuy: '3100000000000000000000',
                    tokenSell: '0x0000000000000000000000000000000000000000',
                    sellPrecision: 18,
                    amountSell: '400000000000000000',
                    expires: 100000,
                    nonce: 1,
                    user: '0x57b080554ebafc8b17f4a6fd090c18fc8c9188a0'
                }
            }

        :raises:  IdexWalletAddressNotFoundException, IdexPrivateKeyNotFoundException, IdexResponseException,  IdexAPIException

        """

        # convert buy and sell amounts based on decimals
        price = self._num_to_decimal(price)
        quantity = self._num_to_decimal(quantity)
        sell_quantity = price * quantity
        amount_buy = self.convert_to_currency_quantity(token_buy, quantity)
        amount_sell = self.convert_to_currency_quantity(token_sell, sell_quantity)

        return self.create_order_wei(token_buy, token_sell, amount_buy, amount_sell)

    def create_order_wei(self, token_buy, token_sell, amount_buy, amount_sell):
        """Create a limit order using buy and sell amounts as integer value precision matching that token

        :param token_buy: The name or address of the token you will receive as a result of the trade e.g. ETH or '0x7c5a0ce9267ed19b22f8cae653f198e3e8daf098'
        :type token_buy: string
        :param token_sell:  The name or address of the token you will lose as a result of the trade e.g. EOS or '0x7c5a0ce9267ed19b22f8cae653f198e3e8daf098'
        :type token_sell: string
        :param amount_buy:  The amount of token_buy you will receive when the order is fully filled
        :type amount_buy: Decimal, string
        :param amount_sell: The amount of token_sell you are selling
        :type amount_sell: Decimal, string

        .. code:: python

            ticker = client.create_order_gwei(
                'EOS',
                'ETH',
                '3100000000000000000000',
                '400000000000000000')

        :returns: API Response

        .. code-block:: python

            {
                orderNumber: 2101,
                orderHash: '0x3fe808be7b5df3747e5534056e9ff45ead5b1fcace430d7b4092e5fcd7161e21',
                price: '0.000129032258064516',
                amount: '3100',
                total: '0.4',
                type: 'buy',
                params: {
                    tokenBuy: '0x7c5a0ce9267ed19b22f8cae653f198e3e8daf098',
                    buyPrecision: 18,
                    amountBuy: '3100000000000000000000',
                    tokenSell: '0x0000000000000000000000000000000000000000',
                    sellPrecision: 18,
                    amountSell: '400000000000000000',
                    expires: 100000,
                    nonce: 1,
                    user: '0x57b080554ebafc8b17f4a6fd090c18fc8c9188a0'
                }
            }

        :raises:  IdexWalletAddressNotFoundException, IdexPrivateKeyNotFoundException, IdexResponseException,  IdexAPIException

        """

        if not self._wallet_address:
            raise IdexWalletAddressNotFoundException()

        if not self._private_key:
            raise IdexPrivateKeyNotFoundException()

        contract_address = self._get_contract_address()

        buy_currency = self.get_currency(token_buy)
        sell_currency = self.get_currency(token_sell)

        hash_data = [
            ['contractAddress', contract_address, 'address'],
            ['tokenBuy', buy_currency['address'], 'address'],
            ['amountBuy', amount_buy, 'uint256'],
            ['tokenSell', sell_currency['address'], 'address'],
            ['amountSell', amount_sell, 'uint256'],
            ['expires', '10000', 'uint256'],
            ['nonce', self._get_nonce(), 'uint256'],
            ['address', self._wallet_address, 'address'],
        ]

        return self._post('order', True, hash_data=hash_data)

    def create_trade(self, order_hash, token, amount):
        """Make a trade

        TODO: Allow multiple orders to be filled

        :param order_hash: This is the raw hash of the order you are filling. The orderHash property of an order can be retrieved from the API calls which return orders, for higher security the has can be derived from the order parameters.
        :type order_hash: string e.g ‘0xcfe4018c59e50e0e1964c979e6213ce5eb8c751cbc98a44251eb48a0985adc52’
        :param token: The name or address of the token you are filling the order with. In the order it's the tokenBuy token
        :type token: string or hex string e.g 'EOS' or '0x7c5a0ce9267ed19b22f8cae653f198e3e8daf098'
        :param amount: This is the amount of the order you are filling  e.g 0.2354
        :type amount: Decimal, string, int or float

        .. code:: python

            trades = client.create_trade(
                '0xcfe4018c59e50e0e1964c979e6213ce5eb8c751cbc98a44251eb48a0985adc52',
                'ETH',
                '1.23')

        :returns: API Response

        .. code-block:: python

            [
                {
                    amount: '0.07',
                    date: '2017-10-13 16:25:36',
                    total: '0.01',
                    market: 'ETH_DVIP',
                    type: 'buy',
                    price: '7',
                    orderHash: '0xcfe4018c59e50e0e1964c979e6213ce5eb8c751cbc98a44251eb48a0985adc52',
                    uuid: '250d51a0-b033-11e7-9984-a9ab79bb8f35'
                }
            ]

        :raises:  IdexWalletAddressNotFoundException, IdexPrivateKeyNotFoundException, IdexResponseException,  IdexAPIException

        """

        if not self._wallet_address:
            raise IdexWalletAddressNotFoundException()

        if not self._private_key:
            raise IdexPrivateKeyNotFoundException()

        amount_trade = self.convert_to_currency_quantity(token, amount)

        hash_data = [
            ['orderHash', order_hash, 'address'],
            ['amount', amount_trade, 'uint256'],
            ['address', self._wallet_address, 'address'],
            ['nonce', self._get_nonce(), 'uint256'],
        ]

        return self._post('trade', True, hash_data=hash_data)

    def cancel_all(self, base, quote):
        book = self.get_order_book(base, quote)
        success = False

        while(not success):
            try:
                for bid in book['bids'].values():
                    if bid['params']['user'] == self._wallet_address:
                        self.cancel_order(base, quote, bid['orderHash'], str(uuid.uuid4()), retries=5,
                                                      exchange_order_id=bid['orderHash'], cb=False)
                for ask in book['asks'].values():
                    if ask['params']['user'] == self._wallet_address:
                        self.cancel_order(base, quote, ask['orderHash'], str(uuid.uuid4()), retries=5,
                                                      exchange_order_id=ask['orderHash'], cb=False)

                success = True
            except Exception as e:
                logger().error('FAILED TO CANCEL ALL OPEN ORDERS: {}'.format(str(e)))
                logger().info("Sleeping 30 seconds then trying again.")
                time.sleep(30)

    def cancel_order(self, base, quote, internal_order_id, request_id, retries=0, exchange_order_id=None, cb=True):
        # cb will be set to false when cancelling unknown orders (ie, orders we thought failed to create but didn't)
        reason = None
        if not self._wallet_address:
            raise IdexWalletAddressNotFoundException()

        if not self._private_key:
            raise IdexPrivateKeyNotFoundException()

        self.pending_cancel[internal_order_id] = True

        order_in_map = False
        order_in_book = False
        # exchange_order_id will be set when cancelling unknown orders
        # (ie, orders we thought failed to create but didn't)
        if exchange_order_id is None:
            if internal_order_id in self.internal_to_external_id:
                exchange_order_id = self.internal_to_external_id[internal_order_id]
                order_in_map = True
            else:
                exchange_order_id = None
                logger().error('Could not cancel order. Order with internal id {} not found in internal->external id map'
                               .format(internal_order_id))
                order_in_map = False
                retries = -1  # To make it fail out immediately

        hash_data = [
            ['orderHash', exchange_order_id, 'address'],
            ['nonce', self._get_nonce(), 'uint256'],
        ]

        json_data = {
            'address': self._wallet_address
        }

        success = False

        while retries >= 0 and not success:
            try:
                response = self._post('cancel', True, hash_data=hash_data, json=json_data)
                success = True
            except Exception as e:
                if not isinstance(e, IdexAPIException):
                    reason = 'Unknown exception type'
                elif hasattr(e, 'message'):
                    if e.message == 'Order no longer available.':
                        retries = -1
                        reason = 'Order not in book'
                        order_in_book = False
                    elif isinstance(e.message, str) and e.message == 'Unusual activity detected, please wait up to an ' \
                                                                     'hour for exchange privileges to be reactivated':
                        retries = -1
                        reason = 'Rate limited'
                    else:
                        reason = str(e.message)
                else:
                    retries = -1
                    reason = 'UNKNOWN'

                if retries > 0:
                    logger().warn('Cancel order attempt failed with reason ({}) and error message: ({}). Retrying'
                                  .format(reason, e.message if hasattr(e, 'message') else str(e)))
                else:
                    logger().error('Cancel order failed with reason ({}) and error message: ({}).'
                                   .format(reason, e.message if hasattr(e, 'message') else str(e)))

                time.sleep(2)
            finally:
                retries = retries - 1

        self.pending_cancel.pop(internal_order_id, None)

        if success:
            # we think that there is undocumented rate limiting...
            time.sleep(2)

            if cb is True:
                self.open_orders.pop(exchange_order_id, None)
                self.internal_to_external_id.pop(internal_order_id, None)
                self.notify_callbacks('trade_lifecycle', data={
                    'action': 'CANCELED',
                    'exchange': self.name,
                    'base': base,
                    'quote': quote,
                    'exchange_order_id': exchange_order_id,
                    'internal_order_id': internal_order_id,
                    'order_status': 'CANCELED',
                    'server_ms': int(round(time.time() * 1000)),
                    'received_ms': int(round(time.time() * 1000))
                })
        elif not order_in_map:
            if cb is True:
                self.open_orders.pop(exchange_order_id, None)
                self.internal_to_external_id.pop(internal_order_id, None)
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
        elif not order_in_book:
            if cb is True:
                self.open_orders.pop(exchange_order_id, None)
                self.internal_to_external_id.pop(internal_order_id, None)
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
        else:
            if cb is True:
                self.notify_callbacks('trade_lifecycle', data={
                    'action': 'CANCEL_FAILED',
                    'reason': reason,
                    'base': base,
                    'quote': quote,
                    'exchange': self.name,
                    'exchange_order_id': exchange_order_id,
                    'internal_order_id': internal_order_id,
                    'order_status': 'OPEN',
                    'server_ms': int(round(time.time() * 1000)),
                    'received_ms': int(round(time.time() * 1000))
                })

    # Withdraw Endpoints

    def withdraw_to_hot_wallet(self, amount, token):
        """Withdraw funds from IDEX to your wallet address

        :param amount:  The amount of token you want to withdraw
        :type amount: Decimal, string
        :param token: The name or address of the token you are withdrawing. In the order it's the tokenBuy token
        :type token: string or hex string e.g 'EOS' or '0x7c5a0ce9267ed19b22f8cae653f198e3e8daf098'

        .. code:: python

            status = client.withdraw('1000.32', 'EOS')

        :returns: API Response

        :raises:  IdexWalletAddressNotFoundException, IdexPrivateKeyNotFoundException, IdexResponseException,  IdexAPIException

        """

        if not self._wallet_address:
            raise IdexWalletAddressNotFoundException()

        if not self._private_key:
            raise IdexPrivateKeyNotFoundException()

        contract_address = self._get_contract_address()

        currency = self.get_currency(token)

        # convert amount
        amount = self.convert_to_currency_quantity(token, amount)

        hash_data = [
            ['contractAddress', contract_address, 'address'],
            ['token', currency['address'], 'address'],
            ['amount', amount, 'uint256'],
            ['address', self._wallet_address, 'address'],
            ['nonce', self._get_nonce(), 'uint256'],
        ]

        return self._post('withdraw', True, hash_data=hash_data)

    #def connect_ws(self):
    #    pass

    #def disconnect_ws(self):
    #    pass

    #def send_via_websocket(self, message):
    #    self.websocket.send(message)

    def follow_market(self, base, quote):
        market = self.to_market(base, quote)
        self.markets_following[market] = True

        #if market not in self.following:
        #    self.books[market] = {'has_snapshot': False, 'book': {}, 'buffer': []}
        #    self.send_via_websocket(json.dumps({'subscribe': market}))
        #    snapshot = self.get_order_book(market)
        #    self.books[market]['has_snapshot'] = True
        #    self.books[market]['book'] = snapshot
        #    for action in self.books[market]['buffer']:
        #        self.process_order_book_action(action['pair'], action['action_type'], action['data'])


    def unfollow_market(self, base, quote):
        key = self.to_market(base, quote)
        self.markets_following.pop(key, None)

    def unfollow_all(self):
        self.markets_following = {}

    #def follow_trades(self, base, quote, callback):
    #    if not self.ws_connected:
    #        self.connect_ws()
    #
    #    key = self.to_market(base, quote)
    #    if key not in self.trades_following:
    #        self.trades_following[key] = callback
    #
    #    if key not in self.following:
    #        self.send_via_websocket(json.dumps({'subscribe': base.upper() + '_' + quote.upper()}))

    #def on_ws_message(self, message):
    #    try:
    #        message = json.loads(message)
    #        if 'message' in message and 'success' in message['message']:
    #            data = message['message']['success']
    #            if type(data) is type(''):
    #                if data.startswith('Subscribed'):
    #                    parts = data.split(' ')
    #                    self.following[parts[2]] = True
    #                elif data.startswith('Unsubscribed'):
    #                    parts = data.split(' ')
    #                    self.following.pop(parts[2], None)
    #                    self.books_following.pop(parts[2], None)
    #                    self.trades_following.pop(parts[2], None)
    #                    self.books.pop(parts[2], None)
    #                    if(len(self.following) == 0):
    #                        self.disconnect_ws()
    #        elif 'topic' in message and 'message' in message and 'type' in message['message'] and\
    #             'data' in message['message']:
    #            message_type = message['message']['type']
    #            data = message['message']['data']
    #            if message_type == 'newTrade' and message['topic'] in self.trades_following:
    #                self.process_execution(data)
    #            elif message['topic'] in self.books_following and (message_type == 'orderBookAdd' or
    #                 message_type == 'orderBookRemove' or message_type == 'orderBookModify'):
    #                self.process_order_book_action(message['topic'], message_type, data)
    #            else:
    #                raise ValueError('Unexpected message. Cannot parse')
    #        else:
    #            raise ValueError('Unexpected message. Cannot parse')
    #    except Exception as e:
    #        logger().error('Failed to process message: {}. Exception was: {}'.format(message, e))

    #def process_order_book_action(self, market, action_type, data):
    #    if market in self.books:
    #        if not self.books[market]['has_snapshot']:
    #            self.books[market]['buffer'].append({'pair': market, 'action_type': action_type, 'data': data})
    #        # Keep a pandas df keyed on order_hash. Return to caller as df keyed on depth
    #        if action_type == 'orderBookRemove':
    #            pass
    #        elif action_type == 'orderBookAdd':
    #            pass
    #        elif action_type == 'orderBookModify':
    #            pass
    #
    #        if self.books[market]['has_snapshot'] and len(self.books[market]['buffer']) == 0:
    #            self.notify_callbacks('order_book', data=self.to_internal_book_format(self.books[market]))

    def notify_callbacks(self, topic, **data):
        for f in self.callbacks.values():
            f(topic, **data)

    def add_callback(self, name, callback):
        self.callbacks[name] = callback

    def remove_callback(self, name):
        del self.callbacks[name]

        #def process_execution(self, data):
        #    # If it's our open order
        #    if data['orderHash'] in self.open_orders:
        #        self.open_orders[data['orderHash']]['cum_quantity_filled'] = \
        #            Decimal(self.open_orders[data['orderHash']]['cum_quantity_filled']) + Decimal(data['amount'])
        #
        #        order_status = 'PARTIALLY_FILLED'
        #        if self.open_orders[data['orderHash']]['cum_quantity_filled'] >= Decimal(self.open_orders['quantity']):
        #            order_status = 'FILLED'
        #
        #        internal_execution = {
        #            'action': 'EXECUTION',
        #            'exchange': self.name,
        #            'exchange_order_id': data['orderHash'],
        #            'internal_order_id': self.open_orders[data['orderHash']]['internal_order_id'],
        #            'side': self.open_orders[data['orderHash']]['side'],
        #            'quantity': self.open_orders[data['orderHash']]['quantity'],
        #            'price': self.open_orders[data['orderHash']]['price'],
        #            'cum_quantity_filled': self.open_orders[data['orderHash']]['cum_quantity_filled'],
        #            'order_status': order_status,
        #            'server_ms': int(round(time.time() * 1000)),
        #            'received_ms': int(round(time.time() * 1000))
        #        }
        #
        #        if order_status == 'FILLED':
        #            self.open_orders.pop([data['orderHash']], None)
        #
        #        # Send to callback
        #        self.notify_callbacks('trade_lifecycle', **internal_execution)

    def get_deposit_address(self, currency):
        return {
            'address': self.get_wallet_address(),
            'tag': None
        }

    def get_deposits(self, currency=None):
        one_week_in_s = 86400 * 31
        now_s = time.time()
        one_week_ago_s = now_s - one_week_in_s
        transfers = self.get_my_transfers(start=one_week_ago_s, end=now_s)
        deposits = transfers['deposits']
        formatted_deposits = []

        for deposit in deposits:
            if currency is None or currency == deposit['currency']:
                formatted_deposits.append({
                    'time': deposit['timestamp'],
                    'asset': deposit['currency'],
                    'amount': Decimal(deposit['amount']),
                    'status': 'complete'
                })

        return formatted_deposits

    def get_withdrawals(self, currency=None):
        one_week_in_s = 86400 * 31
        now_s = time.time()
        one_week_ago_s = now_s - one_week_in_s
        transfers = self.get_my_transfers(start=one_week_ago_s, end=now_s)
        withdrawals = transfers['withdrawals']
        formatted_withdrawals = []

        for withdrawal in withdrawals:
            if currency is None or currency == withdrawal['currency']:
                formatted_withdrawals.append({
                    'time': withdrawal['timestamp'],
                    'asset': withdrawal['currency'],
                    'amount': Decimal(withdrawal['amount']),
                    'status': 'complete' if withdrawal['status'] == 'COMPLETE' else 'pending'
                })

        return formatted_withdrawals

    def withdraw(self, currency, amount, address, tag=None, cb=None):
        self.withdraw_to_hot_wallet(amount, currency)
        # TODO call stage2 on seperate thread after 10 seconds?

    def withdraw_stage2(self, currency, amount, address, time_initiated_s, cb):
        withdraw_to_hot_complete = False
        price_delta = 0.015 # in ETH
        time_delta_s = 10
        if currency != 'ETH':
            book = self._post('returnOrderBook', False,
                              json={'market': self.to_market(currency, 'ETH')},
                              base_url='https://api-regional.idex.market')
            price = book['bids'][0]['price']
            price_delta = price_delta / price

        while not withdraw_to_hot_complete:
            withdrawals = self.get_withdrawals(currency)

            for withdrawal in withdrawals:
                if withdrawal['time'] - time_delta_s >= time_initiated_s and \
                   withdrawal['asset'] == currency and \
                   abs(amount-withdrawal['amount']) <= price_delta and \
                   withdrawal['status'] == 'complete':
                    withdraw_to_hot_complete = True
                    break

            if not withdraw_to_hot_complete:
                time.sleep(60)

        ethereum
        #TODO use ethereum lib to send from exchange hot to address

    def get_public_trades(self, base, quote, start_s, end_s):
        pass

    def get_our_trades(self, base, quote, start_s, end_s):
        pass

if __name__ == '__main__':

    creds = get_creds(secret_name="aye_jay_dev1")
    #creds = {'idex_public': '',
    #         'idex_private': ''}
    c = IdexService('idex', address=creds['idex_pub_prod'], private_key=creds['idex_priv_prod'])

    def cb(topic, **data):
        if topic != 'order_book':
            print(topic + ': ' + str(data))
        else:
            pass
            #print('callback received order_book')

    c.add_callback('sns', cb)

    trades = c.get_trade_history('ETH_UBT', start=1535673600, end=1536278400)
    import csv

    keys = trades[0].keys()

    with open('idex_executions_all.csv', 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(trades)

    # deposits = c.get_deposits()
    # print(deposits)
