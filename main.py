
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
import pandas as pd
nest_asyncio.apply()

from data import orderbooks_df
 

async def get_orderbooks(exchanges, run_time, symbol):
    data = await (orderbooks_df(exchanges=exchanges, symbol=symbol))
    return data

exchanges = ["kucoin", "bittrex", "bitfinex"]
run_time = 100000000000000000000000000000000000000000

#symbol1 = 'BTC/USDT'

control = 0
cc=0

#while (control==0):
    
#    ETH_BTC1 = asyncio.run(get_orderbooks(exchanges, run_time, symbol1))
    
#    if(cc==0):
#        inicial = ETH_BTC1
    
#    if(cc==1):
#        historial = pd.concat([inicial, ETH_BTC1], axis=0)
    
#    if(cc>=2):
#        historial = pd.concat([historial, ETH_BTC1], axis=0)
#        historial.reset_index(drop=True, inplace=True)
#        
        
#    cc+=1

#historial.to_csv('files\orderbooks_27abr2023_BTCUSDT.csv') 
#historial.to_json('files\orderbooks_27abr2023_BTCUSDT.json')

symbol1 = 'ETH/EUR'

while (control==0):
    
    ETH_BTC1 = asyncio.run(get_orderbooks(exchanges, run_time, symbol1))
    
    if(cc==0):
        inicial = ETH_BTC1
    
    if(cc==1):
        historial = pd.concat([inicial, ETH_BTC1], axis=0)
    
    if(cc>=2):
        historial = pd.concat([historial, ETH_BTC1], axis=0)
        historial.reset_index(drop=True, inplace=True)
                
    cc+=1

#historial.to_json('files\orderbooks_27abr2023_ETHEUR.json')
#historial.to_csv('files\orderbooks_27abr2023_ETHEUR.csv')    








#print(ETH_BTC.info())

