
"""
# -- --------------------------------------------------------------------------------------------------- -- #
# -- project: A SHORT DESCRIPTION OF THE PROJECT                                                         -- #
# -- script: main.py : python script with the main functionality                                         -- #
# -- author: YOUR GITHUB USER NAME                                                                       -- #
# -- license: GPL-3.0 License                                                                            -- #
# -- repository: YOUR REPOSITORY URL                                                                     -- #
# -- --------------------------------------------------------------------------------------------------- -- #
"""

import asyncio
import nest_asyncio
nest_asyncio.apply()

from data import orderbooks_df

async def get_orderbooks(exchanges, run_time, symbol):
    #data = await (orderbooks_df(exchanges=exchanges, run_time=run_time, symbol=symbol))
    data = await (orderbooks_df(exchanges=exchanges))
    return data

exchanges = ['bitforex', 'hubipro', 'bitmart']
#runt_time = 3600
run_time = 10

symbol = 'ETH/BTC'
ETH_BTC = asyncio.run(get_orderbooks(exchanges, run_time, symbol))
ETH_BTC.to_json('files\orderbooks_23abr2023.json')

print(ETH_BTC.info())

