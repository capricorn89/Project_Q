#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep  2 23:27:23 2019

@author: Woojin
"""


##############################################
# Initial Settings
##############################################

import pandas as pd
import numpy as np
import os, inspect
from scipy import stats
import statsmodels.tsa.stattools as ts
import time
import sqlite3
from tqdm import tqdm

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

os.chdir(path + '/__statPairs__')
import stat_util as util
os.chdir(path + '/__Database__')
import DB_util as db

DBName = 'DB_ver_1.3.db'
dbData = db.get_DB(DBName)
#os.chdir(path)

def pairData(tickers, period='all', *periods):
    
    if period == 'all':
        today = pd.datetime.strftime(pd.datetime.today(),"%Y%m%d")
        df = dbData.from_econ(tickers, '00000000', today)
    
    elif period == 'specific':    
        start_ = periods[0]
        end_ = periods[1]
        df = dbData.from_econ(tickers, start_, end_)
    
    return df

conn = sqlite3.connect(path + '/__Database__/' + DBName)


##############################################
# Make all combinations
##############################################
import itertools
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler

all_tickers = list(pd.read_sql_query("SELECT ticker FROM info", conn)['ticker'].values)
combs = [ticks for ticks in  itertools.combinations(all_tickers, 2)]

##############################################
# 1. Euclidian Distance
##############################################
'''일단 3년치 월간 데이터로 테스트'''
yearAgo_3 = '20150930' 
today = pd.datetime.strftime(pd.datetime.today(),"%Y%m%d")

def normalize(series):
    val = series.values
    val = val.reshape((len(val), 1))
    scaler = StandardScaler()
    scaler = scaler.fit(val)
    normalized = scaler.transform(val)
    return normalized

def normalize_df(df):
    for col in df.columns:
        df.loc[:,col] = normalize(df[col])
    return df


# Get Distance
def get_ssd(pairData):
    ssd = sum((pairData.iloc[:,0] - pairData.iloc[:,1]).dropna().values ** 2)
    return ssd

# Save SSD of all pair
    
ticker_1 = []
ticker_2 = []
distance = []
corr = []


for i in tqdm(range(len(combs))):
    
    combi  = list(combs[i])
    df = pairData(combi, 'specific', yearAgo_3, today)  # 월간 데이터라는 가정하에
    df = df.dropna()
    if (len(df.columns) < 2) | (len(df) < 10):
        pass
    else:
        df = df.resample('M').last()
        
        norm_df = normalize_df(df)
        ticker_1.append(df.columns[0])
        ticker_2.append(df.columns[1])
        distance.append(get_ssd(norm_df))
        corr.append(df.pct_change().corr().values[1,0])


res = pd.DataFrame({'ticker_1':ticker_1, 'ticker_2':ticker_2, 'SSD' : distance,
                    'correlation' : corr})

#combi_test = ('USCONPRCE', 'USCYLEADQ')
#df = pairData(combi_test, 'specific', yearAgo_3, today)
#
#test = ('AUWGEINFB', 'BRGPIM10F')
#df = pairData(test, 'specific', yearAgo_3, today)
