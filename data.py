import asyncio
import nest_asyncio
nest_asyncio.apply()
import ccxt.async_support as ccxta
import time
import numpy as np
import pandas as pd


async def async_client(exchange_id, run_time: int, symbol: str):
    orderbook = None
    exchange = getattr(ccxta, exchange_id)()
    time_1 = time.time()
    time_f = 0
    ob = []
    while time_f <= run_time:
        try:
            await exchange.load_markets()
            market = exchange.market(symbol)
            orderbook = await exchange.fetch_order_book(market["symbol"])
            ohlc = await exchange.fetch_ohlcv(symbol=market['symbol'], timeframe='1m')
            datetime = exchange.iso8601(exchange.milliseconds())
            # Unpack values
            bid = orderbook['bids'][0][0] if len (orderbook['bids']) > 0 else None # Get the highest bid
            ask = orderbook['asks'][0][0] if len (orderbook['asks']) > 0 else None # Get the lowest ask
            
            bid_volume = sum([bid[1] for bid in orderbook['bids']]) if len (orderbook['bids']) > 0 else None # Get the total bid volume
            ask_volume = sum([ask[1] for ask in orderbook['asks']]) if len (orderbook['asks']) > 0 else None # Get the total ask volume
            total_volume = bid_volume + ask_volume

            
            mid_price = (bid+ask)/2
            spread = np.round(ask - bid, 4)
            # Final data format for the results
            ob.append(
                {
                    "exchange": exchange_id,
                    "datetime": datetime,
                    "level": len(orderbook['asks']),
                    "bid_volume": bid_volume,
                    "ask_volume": ask_volume,
                    "total_volume": total_volume,
                    "mid_price": mid_price,
                    "spread": spread,
                    "close_price": ohlc[0][4],
                    "Symbol": symbol, #placeholder
                    
                    #"orderbook": {
                     #   "ask_size": ask_size.tolist(),
                      #  "ask": ask_price.tolist(),
                       # "bid": bid_price.tolist(),
                        #"bid_size": bid_size.tolist(),
                        #"spread": spread.tolist(),
                    #},
                }
            )
            # End time
            time_2 = time.time()
            time_f = round(time_2 - time_1, 4)
        except Exception as e:
            time_2 = time.time()
            time_f = round(time_2 - time_1, 4)
            print(type(e).__name__, str(e))
    await exchange.close()
    return ob


async def multi_orderbooks(exchanges, run_time: int, symbol: str):
    input_coroutines = [
        async_client(exchange, run_time, symbol) for exchange in exchanges
    ]
    orderbooks = await asyncio.gather(*input_coroutines, return_exceptions=True)
    return orderbooks

#if __name__ == "__main__":
#    exchanges = ["kucoin", "bittrex", "bitfinex", "poloniex", "huobipro"]
#    run_time = 10  # seconds
#    symbol = "ETH/BTC"
#    data = asyncio.run(multi_orderbooks(exchanges, run_time=run_time, symbol=symbol))
#    data = [item for sublist in data for item in sublist]
#    data = pd.DataFrame(data)
#
#    print(data.head())
    
#async def orderbooks_df(exchanges:list, run_time:int, symbol:str):
##     exchanges = ["kucoin", "bittrex", "bitfinex", "poloniex", "huobipro"]
##     run_time = 10  # seconds
##     symbol = "ETH/BTC"
#
#     data = asyncio.run(multi_orderbooks(exchanges, run_time=run_time, symbol=symbol))
#     data = [item for sublist in data for item in sublist]
#     data = pd.DataFrame(data)
#     data.set_index('exchange', inplace=True)
#
#     return(data)
     
async def orderbooks_df(exchanges:list, symbol:str):
    exchanges = ["kucoin", "bittrex", "bitfinex"]
    run_time = 10  # seconds
    #symbol = "BTC/USDT"

    #symbol = "ETH/EUR"
    
    
    data = asyncio.run(multi_orderbooks(exchanges, run_time=run_time, symbol=symbol))
    data = [item for sublist in data for item in sublist]
    data = pd.DataFrame(data)

    return(data)
