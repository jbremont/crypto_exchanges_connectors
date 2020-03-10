import threading
from time import sleep

import websocket


class OrderBookSocket(object):
    def __init__(self):
        websocket.enableTrace(True)
        self.books_following = {}
        self.ws = websocket.WebSocketApp("wss://real.okex.com:10441/websocket",
                                         on_message= self.on_message,
                                         on_error= self.on_error,
                                         on_close= self.on_close,
                                         on_open=self.on_open)
        self.open = False
        thread = threading.Thread(target=self.ws.run_forever, args=())
        thread.daemon = False
        thread.start()

        while self.open is False:
            sleep(1)

    def add_subscription(self, market):
        if market not in self.books_following:
            self.books_following[market] = True
            self.ws.send("{'event':'addChannel','channel':'ok_sub_spot_" + market + "_depth_20'}")

    def remove_subscription(self, market):
        self.books_following.pop(market, None)

    def on_message(self, ws, message):
        print(message)

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print("### Websocket closed ###")

    def on_open(self, ws):
        self.open = True
        print('Websocket opened!')
