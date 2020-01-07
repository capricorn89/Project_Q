# -*- coding: utf-8 -*-
"""
Created on Fri Dec 13 15:01:38 2019

@author: Woojin
"""
##############################################################################
# 0. Settings...
##############################################################################
import os, inspect
import pandas as pd
import numpy as np
import itertools
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
import platform

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(path)

##############################################################################
# 1. Load Data (DB로 만드는 경우 이 부분은 수정 필요)
##############################################################################

import dataload
nameData, econ_data, index_data, econ_ticker, index_ticker = dataload.get_data()

##############################################################################
# 2. 각 경제지표별로 지수와의 유사도 측정
##############################################################################    
def normalize(series):
    val = series.values
    val = val.reshape((len(val), 1))
    scaler = StandardScaler()
    scaler = scaler.fit(val)
    return scaler.transform(val)

def normalize_df(df):
    for col in df.columns:
        df.loc[:,col] = normalize(df[col])
    return df

def get_ssd(pair):
    ssd = sum((pair.iloc[:,0] - pair.iloc[:,1]).dropna().values ** 2)
    return ssd

ticker_econ = []
ticker_index = []
distance_longer = []
distance_recent = []

lookback_1 = 2  # 최근 3년 기준
lookback_2 = 5  # 최근 3년 기준
start_date = pd.datetime.today() - pd.DateOffset(years = lookback_1)
older_date = pd.datetime.today() - pd.DateOffset(years = lookback_2)
for i in range(len(econ_data.keys())):
#    print(list(econ_data.keys())[i])
    for j in range(len(index_data.keys())):
        # 1) 각 데이터 하나씩 불러오기
        econ_tk = list(econ_data.keys())[i]
        index_tk = list(index_data.keys())[j]
        econ_1 = econ_data[econ_tk]
        index_1 = index_data[index_tk]
        
        # 2) 경제지표 데이터 주기에 맞춰서 데이터 리샘플링 후 Concat
        period = econ_ticker[econ_ticker.Ticker == econ_tk].Period.values[0]
        econ_1 = econ_1.resample(period).last()
        index_1 = index_1.resample(period).last()
        pairData_original = pd.concat([econ_1, index_1], axis = 1).dropna()    
        pairData = pairData_original.loc[start_date:, :]
        olderData = pairData_original.loc[older_date:, :]

        # 3) 표준화
        normPairData = normalize_df(pairData)
        normOlderData = normalize_df(olderData)
        # 4) 유사도 측정
        # 4-1) Distance Method
        ssd = get_ssd(normPairData)        
        ssd_older = get_ssd(normOlderData)
        # 5) 저장
        ticker_econ.append(econ_tk)
        ticker_index.append(index_tk)
        distance_recent.append(ssd)
        distance_longer.append(ssd_older)

df_ssd = pd.DataFrame({'econ':ticker_econ, 
                       'EquityIndex': ticker_index,
                       'SSD_recent' : distance_recent,
                       'SSD_older' : distance_longer})

df_ssd['SSD_diff'] = df_ssd['SSD_older'] - df_ssd['SSD_recent']
df_ssd.sort_values(by = 'SSD_diff', inplace=True)
df_ssd = df_ssd.reset_index(drop=True)

##############################################################################
# 3. Pair 별로 저장 (Database화를 염두에 두고 작업해야함)
##############################################################################    

    




##############################################################################
# 4. Chart 저장 후 Plot 
##############################################################################    
import chart

def makePair(econTicker, indexTicker, startDate):
    econ_1 = econ_data[econTicker]
    index_1 = index_data[indexTicker]
    period = econ_ticker[econ_ticker.Ticker == econ_tk].Period.values[0]
    econ_1 = econ_1.resample(period).last()
    index_1 = index_1.resample(period).last()
    pairData = pd.concat([econ_1, index_1], axis = 1)#.dropna()        
    pairData = pairData.loc[startDate:pd.datetime.today(), :]
    return pairData

rank = 1
pairs_ticker = df_ssd.iloc[rank,:2].values
names = [nameData.loc[t].values[0] for t in pairs_ticker]

pairs = makePair(pairs_ticker[0], pairs_ticker[1], start_date)
chart.dualAxesPlot(pairs, 'pair_1', names)
