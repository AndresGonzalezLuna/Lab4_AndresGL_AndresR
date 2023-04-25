
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

def vwap(df):
    
    df['Price*Volume'] = df['mid_price'] * df['total_volume']

    # Calculate the cumulative sum of Price*Volume and Volume
    df['Cumulative Price*Volume'] = df['Price*Volume'].cumsum()
    df['Cumulative Volume'] = df['total_volume'].cumsum()
    
    # Calculate VWAP
    df['VWAP'] = df['Cumulative Price*Volume'] / df['Cumulative Volume']
    
    df = df.drop(['Price*Volume', 'Cumulative Price*Volume', 'Cumulative Volume'], axis=1)
    
    #df = df[df.columns[:-3]]
    
    return df

def rolls(df):
    
    df.set_index('datetime', inplace=True)
    
    # calculate the returns and lag them by one time period
    df['returns'] = np.log(df['mid_price']).diff()
    
    #Arreglar el shift para que sea de 1 minuto
    df['lagged_returns'] = df['returns'].shift(1)
    
    # calculate the covariance matrix of the returns and lagged returns
    cov_matrix = np.cov(df[['returns', 'lagged_returns']].dropna().T)
    
    # calculate Roll's spread
    roll_spread = np.sqrt(cov_matrix[0, 0])
    
    # apply Roll's spread to the timeseries
    df['roll_spread'] = roll_spread*2
    
    df = df.drop(['returns', 'lagged_returns', 'Price*Volume', 'Cumulative Price*Volume', 'Cumulative Volume'], axis=1)
    
    return df