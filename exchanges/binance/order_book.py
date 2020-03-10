import sched
import time
from decimal import Decimal

from binance.websockets import BinanceSocketManager
from datetime import datetime


    
class OrderBookService(object):

    def __init__(self, client, base, quote, callback, name):
        self.binance_client = client
        self.buffer = []
        self.recovered = False
        self.master_order_book = {'bids': dict(), 'asks': dict()}
        self.last_update_id_processed = 0
        self.base = base
        self.quote = quote
        self.cross = base + quote
        self.schedule = sched.scheduler(time.time, time.sleep)
        self.conn_key = None        
        self.last_update_time = None
        self.callback = callback
        self.name = name
        
    def start(self):
        self.bm = BinanceSocketManager(self.binance_client)
        self.conn_key = self.bm.start_depth_socket(self.cross, self.__process_depth_message)
        self.bm.start()
        time.sleep(2)
        
    def stop(self):
        #TODO figure out how to split code so that you can call start, stop, start etc.
        self.bm.stop_socket(self.conn_key)
        self.bm.close()
        
    def __notify(self):
        b = self.get_order_book()
        self.callback('order_book', data=b)

    def get_order_book(self):
        sorted_book = self.__sort_order_book(self.master_order_book)
        sorted_book['base'] = self.base
        sorted_book['quote'] = self.quote
        sorted_book['exchange'] = self.name

        return sorted_book

    @staticmethod
    def __sort_order_book(unsorted_order_book):
        sorted_book = {'bids': [], 'asks': []}

        for k in sorted(unsorted_order_book['bids'], reverse=True):
            sorted_book['bids'].append([k, unsorted_order_book['bids'][k]])

        for k in sorted(unsorted_order_book['asks']):
            sorted_book['asks'].append([k, unsorted_order_book['asks'][k]])

        return sorted_book

    def __apply_snapshot(self, snapshot):
        self.master_order_book = {'bids': dict(), 'asks': dict()}
        for bid in snapshot['bids']:
            self.master_order_book['bids'][bid[0]] = bid[1]
        for ask in snapshot['asks']:
            self.master_order_book['asks'][ask[0]] = ask[1]

        self.last_update_id_processed = snapshot['lastUpdateId']

    def __process_update(self, update):
        for bid in update['b']:
            if Decimal(bid[1]) == 0:
                self.master_order_book['bids'].pop(bid[0], None)
            else:
                self.master_order_book['bids'][bid[0]] = bid[1]
        for ask in update['a']:
            if Decimal(ask[1]) == 0:
                self.master_order_book['asks'].pop(ask[0], None)
            else:
                self.master_order_book['asks'][ask[0]] = ask[1]
        self.last_update_id_processed = update['u']
        self.last_update_time = datetime.utcnow()
        self.__notify()
        
        
    def __process_snapshot(self, last_update_id, snapshot):
        if snapshot['lastUpdateId'] < last_update_id:
            print('Snapshot received is too old (last update id before recover triggered is more recent). '
                  'Retrying in 5 seconds.')
            self.schedule.enter(5000, 1, self.__recover_from_snapshot(last_update_id))
        else:
            self.__apply_snapshot(snapshot)
            # Process the buffered deltas
            for update in self.buffer:
                if update['U'] <= self.last_update_id_processed + 1 <= update['u']:
                    self.__process_update(update)
            self.recovered = True
            self.buffer.clear()
            print('Recovery from snapshot complete. Continuing to parse updates via WebSocket')
            self.__notify()
            
    def __recover_from_snapshot(self, last_update_id):
        print('Recover from snapshot triggered')
        snapshot = self.binance_client.get_order_book(symbol=self.cross)
        self.__process_snapshot(last_update_id, snapshot)

    def __process_depth_message(self, msg):
        if not self.recovered:
            # If first depth message received, trigger snapshot recovery
            if len(self.buffer) == 0:
                self.buffer.append(msg)
                self.__recover_from_snapshot(msg['u'])
            else:
                # Warning - There is potential to get a gap here. Edge case, so avoiding for now
                self.buffer.append(msg)
        else:
            if msg['u'] >= self.last_update_id_processed + 1:
                self.__process_update(msg)
                if msg['U'] > (self.last_update_id_processed + 1):
                    print('Encountered gap in updates. Last update id recorded: {}. '
                          'First update id in this new message: {}. '
                          'Last update id in this new message: {}'.format(self.last_update_id_processed, msg['U'],
                                                                          msg['u']))
                    self.recovered = False
                    self.buffer.append(msg)
                    self.__recover_from_snapshot(msg['u'])


example_sorted_book = {'bids': [[100.4, 100], [100.3, 50], [100.2, 200]], 'asks': [[101.2, 30], [101.3, 100], [101.4, 50]]}