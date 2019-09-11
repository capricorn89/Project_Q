# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 15:25:52 2019

@author: Woojin
"""

##  Eikon 사용 가능 티커 정리

import eikon as ek
import pandas as pd
import numpy as np
import os, inspect
import arrow

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(path)

tickers = pd.read_excel('tickerList.xlsx')
tickers = tickers[tickers['ticker'] != 'DUMMY']

username_ = input("USERNAME : ")
pw_ = input("PASSWORD : ") 


# 1) For econ table
import DatastreamDSWS as DSWS
ds = DSWS.Datastream(username = username_, password = pw_)
df_list_2 = []
na_ticker = []
error_ticker = []
for i in range(len(tickers)):
    ticker = list(np.squeeze(tickers['ticker']))[i]
    flds = list(np.squeeze(tickers['field']))[i]
    try:
        df =ds.get_data(ticker, fields = ['X'], start="-100D", end="-0D", freq="D")
        if df.shape[1] == 3:
            na_ticker.append(ticker)
            pass
        else:
            df.columns = [ticker]
            df_list_2.append(df)
            print(ticker)
    except:
        error_ticker.append(ticker)
        pass

final_df = pd.concat(df_list_2,axis=1).stack().reset_index()
final_df.columns = ['date', 'ticker', 'value']
final_df.set_index('date', inplace=True)
final_df = final_df.sort_values('date')
final_df.to_excel('sample_DBformat.xlsx')


# 2) For info table
ticker_list = []
country_list = []
name_list = []
NDOR_date_list = []
NDOR_time_list = []
NDOR_refDate_list = []
for i in range(len(tickers)):
    ticker = tickers.loc[i, 'ticker']
    print(ticker)
    
    country = ds.get_data(tickers = ticker, fields=["GEOGN"], kind = 0)['Value'].values[0]
    name = ds.get_data(tickers = ticker, fields=["NAME"], kind = 0)['Value'].values[0]
    
    
    ticker_list.append(ticker)
    country_list.append(country)
    name_list.append(name)    
    
    df = ds.get_data(tickers = ticker, fields=["DS.NDOR1"])
    date_val = df[df['Datatype'] == 'DS.NDOR1_DATE']['Value'].values[0]
    time_val = df[df['Datatype'] == 'DS.NDOR1_TIME_GMT']['Value'].values[0]
    ref_date_val = df[df['Datatype'] == 'DS.NDOR1_REF_PERIOD']['Value'].values[0]    
    
    if time_val != 'NA':
        seoul_dt = arrow.get(date_val + ' ' + time_val, 'YYYY-MM-DD HH:mm').to('Asia/Tokyo').datetime
        seoul_date = int(pd.to_datetime(seoul_dt).strftime("%Y%m%d"))
        seoul_time = int(pd.to_datetime(seoul_dt).strftime("%H%M"))  

    elif (time_val == 'NA') & (date_val != 'NA'):
        seoul_dt = arrow.get(date_val + ' ' + time_val, 'YYYY-MM-DD').datetime
        seoul_date = int(pd.to_datetime(seoul_dt).strftime("%Y%m%d"))
        seoul_time = np.NaN  
        
    elif (time_val == 'NA') & (date_val == 'NA'):
        seoul_date = np.NaN
        seoul_time = np.NaN  
    
    else:
        pass

    if ref_date_val != 'NA':
        ref_date_val = int(str(ref_date_val[:4])+str(ref_date_val[5:7])+ref_date_val[8:10])
    else:
        ref_date_val = np.NaN
    
    NDOR_date_list.append(seoul_date)
    NDOR_time_list.append(seoul_time)
    NDOR_refDate_list.append(ref_date_val)
    
    
finalInfo_df = pd.DataFrame({'ticker':ticker_list,
                             'name' : name_list,
                             'country':country_list,
                             'NDOR_date' : NDOR_date_list,
                             'NDOR_time' : NDOR_time_list,
                             'NDOR_ref_date' :NDOR_refDate_list})
    
finalInfo_df.to_excel('infoData.xlsx')