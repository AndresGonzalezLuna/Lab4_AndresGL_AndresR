import asyncio
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
            datetime = exchange.iso8601(exchange.milliseconds())
            # Unpack values
            bid_price = orderbook['bids'][0][0] if len (orderbook['bids']) > 0 else None # Get the highest bid
            ask_price = orderbook['asks'][0][0] if len (orderbook['asks']) > 0 else None # Get the lowest ask
            

            bid_volume = sum([bid[1] for bid in orderbook['bids']]) if len (orderbook['bids']) > 0 else None # Get the total bid volume
            ask_volume = sum([ask[1] for ask in orderbook['asks']]) if len (orderbook['asks']) > 0 else None # Get the total ask volume
            total_vol = bid_volume + ask_volume

            mid_price = (bid_price + ask_price) / 2 # Calculate the mid price
            
            
            # Define the time frame for the data
            timeframe = '10s'

            # Define the start and end times for the data
            end_time = exchange.milliseconds()
            start_time = end_time - 1000 * 10  # 10 seconds ago
            # Calculate the VWAP
            
            # Get the historical OHLCV data
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, start_time, limit=1000)
            vwap = 0.0
            total_volume = 0.0
            for candle in ohlcv:
                volume = candle[5]
                total_volume += volume
                vwap += candle[2] * volume
            
            if total_volume > 0:
                vwap /= total_volume



            
            
            mid_price = (float(bid_price) + float(ask_price)) / 2
            spread = np.round(ask_price - bid_price, 4)
            # Final data format for the results
            ob.append(
                {
                    "exchange": exchange_id,
                    "timeStamp": datetime,
                    "Level": len(orderbook['asks']),
                    'ask_Volume': ask_volume,
                    'bid_Volumne': bid_volume,
                    'total_vol': total_vol,
                    'Mid_price': mid_price,
                    'VWAP': vwap,
                    'Spread': spread,
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


# if __name__ == "__main__":
#     exchanges = ["kucoin", "bittrex", "bitfinex", "poloniex", "huobipro"]
#     run_time = 10  # seconds
#     symbol = "ETH/BTC"

#     data = asyncio.run(multi_orderbooks(exchanges, run_time=run_time, symbol=symbol))
#     data = [item for sublist in data for item in sublist]
#     data = pd.DataFrame(data)

#     print(data.head())
