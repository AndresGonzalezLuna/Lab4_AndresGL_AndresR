
"""
# -- --------------------------------------------------------------------------------------------------- -- #
# -- project: A SHORT DESCRIPTION OF THE PROJECT                                                         -- #
# -- script: functions.py : python script with general functions                                         -- #
# -- author: YOUR GITHUB USER NAME                                                                       -- #
# -- license: GPL-3.0 License                                                                            -- #
# -- repository: YOUR REPOSITORY URL                                                                     -- #
# -- --------------------------------------------------------------------------------------------------- -- #
"""
import numpy as np
import pandas as pd

def vwap(df):
    
    dfs = []
    
    for exchange in list(df['exchange'].unique()):
    
        df4 = df[df['exchange'] == exchange].copy()
    
        df4['Price*Volume'] = df4['mid_price'] * df4['total_volume']
    
        # Calculate the cumulative sum of Price*Volume and Volume
        df4['Cumulative Price*Volume'] = df4['Price*Volume'].cumsum()
        df4['Cumulative Volume'] = df4['total_volume'].cumsum()
        
        # Calculate VWAP
        df4['VWAP'] = df4['Cumulative Price*Volume'] / df4['Cumulative Volume']
        
        df4 = df4.drop(['Price*Volume', 'Cumulative Price*Volume', 'Cumulative Volume'], axis=1)
        
        dfs.append(df4)
    
    #df = df[df.columns[:-3]]
    
    return pd.concat(dfs, ignore_index=True)

def rolls(df):
    
    
    dfs = []
    
    for exchange in list(df['exchange'].unique()):
        
        df4 = df[df['exchange'] == exchange].copy()

        df4.set_index('datetime', inplace=True)
        
        # calculate the returns and lag them by one time period
        df4['returns'] = np.log(df4['close_price']).diff()
        
        #Arreglar el shift para que sea de 1 minuto
        df4['lagged_returns'] = df4['returns'].shift(1)
        
        # calculate the covariance matrix of the returns and lagged returns
        cov_matrix = np.cov(df4[['returns', 'lagged_returns']].dropna().T)
        
        # calculate Roll's spread
        roll_spread = np.sqrt(cov_matrix[0, 0])
        
        # apply Roll's spread to the timeseries
        df4['roll_spread'] = roll_spread*2
        
        df4 = df4.drop(['returns', 'lagged_returns'], axis=1)
        
        dfs.append(df4)
        
    return pd.concat(dfs, ignore_index=True)