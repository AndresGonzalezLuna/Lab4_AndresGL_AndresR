# -*- coding: utf-8 -*-
"""
Created on Wed Apr 26 08:25:21 2023

@author: jonha
"""

import asyncio
import nest_asyncio
nest_asyncio.apply()

from data import orderbooks_df
 

async def get_orderbooks(exchanges, run_time, symbol):
    #data = await (orderbooks_df(exchanges=exchanges, run_time=run_time, symbol=symbol))
    data = await (orderbooks_df(exchanges=exchanges, symbol=symbol))
    return data

exchanges = ["kucoin", "bittrex", "bitfinex"]
run_time = 10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
#run_time = 10

symbol2 = 'ETH/EUR'

ETH_BTC2 = asyncio.run(get_orderbooks(exchanges, run_time, symbol2))
ETH_BTC2.to_json('files\orderbooks_23abr2023_ETHEUR.json')
