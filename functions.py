
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

def rolls(df, lag_minutes):
    
    dfs = []
    
    for exchange in list(df['exchange'].unique()):
        
        df4 = df[df['exchange'] == exchange].copy()
        
        df4.set_index('datetime', inplace=True)
        
        df4.index = pd.to_datetime(df4.index) # hacemos index datetime object
        
        # get data by second
        df4 = df4.resample('T').mean()
        
        # get lag minutes intervals in data    

        hour = pd.to_datetime(df4.index.hour, format='%H')
        minute = pd.to_datetime(df4.index.minute // lag_minutes * lag_minutes, format='%M')

        df4['5min'] = (hour + pd.TimedeltaIndex(minute.hour * 60 + minute.minute, unit='m')).strftime('%H:%M')

        minutes5 = list(df4['5min'].unique())
        
        # Sacamos lag para cad bin que acabos de crear
        
        pairs = []

        for i in range(1, len(minutes5)):
        
            returns = np.array(np.log(df4.loc[df4['5min'] == minutes5[i], 'close_price']).diff().dropna()) #Pt
            lagged_returns = np.array(np.log(df4.loc[df4['5min'] == minutes5[i-1], 'close_price']).diff().dropna()) #Pt-1
            
            arrays = [returns, lagged_returns]
            arrays = sorted(arrays, key=lambda x:len(x)) # ordenamos de chico a grande por length
            
            arrays[0] = np.pad(arrays[0], (0, len(arrays[1]) - len(arrays[0])), mode='constant') # agregamos ceros para que el legth coincida
            
            cov = np.cov(arrays[0], arrays[1])[0,1] # Sacamos covarianza entre Pt y Pt-1
            
            roll_spread =2*np.sqrt(abs(cov)) # Sacamos Rolls Effective Spread
            
            pair = [minutes5[i], roll_spread] # Creamos par de bin con su respectivo Rolls
            
            pairs.append(pair) # Agregmaos a lista
            
            
            
        df_pairs = pd.DataFrame(pairs, columns = ['5min', 'Rolls Spread']) 
        
        df4['Time'] = df4.index
        
        df_final = pd.merge(df4, df_pairs, on=['5min'], how = 'outer')
        
        df_final.set_index('Time', inplace=True)
        
        df_final = df_final.resample(str(lag_minutes)+'T').mean()
        
        df_final['exchange'] = exchange
        
        dfs.append(df_final)
        
    #return pd.concat(dfs, ignore_index=True) #Juntamos todos los Df
    return dfs