import json
from exchanges.binance import BinanceService

bs = BinanceService()
bs.follow_order_book("BNB", "BTC")
bs.follow_order_book("BNB", "USDT")
bs.follow_order_book("BTC", "USDT")
bs.follow_order_book("ETH", "BTC")
bs.follow_order_book("ETH", "USDT")
bs.follow_order_book("NEO", "BNB")
bs.follow_order_book("NEO", "BTC")
bs.follow_order_book("NEO", "ETH")
bs.follow_order_book("NEO", "USDT")


def strip_and_write(order_book, file_):
    del order_book['timestamp']
    as_json = order_book.to_dict(orient='records')
    file_.write(json.dumps(as_json))


with open('bnb_btc.json', 'w') as file:
    strip_and_write(bs.get_order_book("BNB", "BTC"), file)


with open('bnb_usdt.json', 'w') as file:
    strip_and_write(bs.get_order_book("BNB", "USDT"), file)


with open('btc_usdt.json', 'w') as file:
    strip_and_write(bs.get_order_book("BTC", "USDT"), file)


with open('eth_btc.json', 'w') as file:
    strip_and_write(bs.get_order_book("ETH", "BTC"), file)


with open('eth_usdt.json', 'w') as file:
    strip_and_write(bs.get_order_book("ETH", "USDT"), file)


with open('neo_bnb.json', 'w') as file:
    strip_and_write(bs.get_order_book("NEO", "BNB"), file)


with open('neo_btc.json', 'w') as file:
    strip_and_write(bs.get_order_book("NEO", "BTC"), file)


with open('neo_eth.json', 'w') as file:
    strip_and_write(bs.get_order_book("NEO", "ETH"), file)


with open('neo_usdt.json', 'w') as file:
    strip_and_write(bs.get_order_book("NEO", "USDT"), file)
