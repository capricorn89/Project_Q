# -*- coding: utf-8 -*-
"""
Created on Thu Jan 24 15:42:30 2019

@author: Woojin Ji
"""

# Find Cointegration



import pandas as pd
import numpy as np
import os, inspect
from scipy import stats
import statsmodels.tsa.stattools as ts
import time


# For whole period
def coint(x_, y_):
    return ts.coint(x_,y_)

def coint_lead(x_, y_, period_y_lead):
    n = period_y_lead
    if n > 0: # x가 y보다 선행하는 경우
        x_ = x_.iloc[n:,]   # X는 원 시계열 (앞에서부터 자름)
        y_ = y_.shift(-n).iloc[:-n]   # y는 원시계열을 앞으로 당김   
        y_.index = x_.index
    elif n < 0: # y가 x보다 선행하는 경우
        n = -n
        x_ = x_.shift(-n).iloc[:-n]
        y_ = y_.iloc[n:,]
        x_.index = y_.index
    else:
        pass

    return ts.coint(x_, y_)

# For moving windows
def moving_coint(x_, y_, window_):    
    dateIndex = x_.index  # 시계열 보관
    df = pd.concat([x_, y_], axis=1).reset_index().iloc[:,1:]  # 시계열 인덱스를 없앤 디폴트
    df.columns = ['X', 'Y']  
    df['p_val'] = range(len(df))
    df['p_val'] = df['p_val'].rolling(window_).apply(lambda ii: ts.coint(df.loc[ii, 'X'], df.loc[ii, 'Y'])[1])
    df.index = dateIndex    
    return df

# For moving window and lead
def moving_coint_lead(x_, y_, window_, period_y_lead):
    n = period_y_lead
    if n > 0: # x가 y보다 선행하는 경우
        x_ = x_.iloc[n:,]   # X는 원 시계열 (앞에서부터 자름)
        y_ = y_.shift(-n).iloc[:-n]   # y는 원시계열을 앞으로 당김   
        y_.index = x_.index
    elif n < 0: # y가 x보다 선행하는 경우
        n = -n
        x_ = x_.shift(-n).iloc[:-n]
        y_ = y_.iloc[n:,]
        x_.index = y_.index
    else:
        pass
    dateIndex = x_.index
    df = pd.concat([x_, y_], axis=1).reset_index().iloc[:,1:]  # 시계열 인덱스를 없앤 디폴트
    df.columns = ['X', 'Y']  
    df['p_val'] = range(len(df))
    df['p_val'] = df['p_val'].rolling(window_).apply(lambda ii: ts.coint(df.loc[ii, 'X'], df.loc[ii, 'Y'])[1])
    df.index = dateIndex  
    return df

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(path)

metaData = pd.read_csv('FRED_metadata.csv')
recent = metaData.loc[metaData['refreshed_at']>'2019-01-01'].reset_index().iloc[:,1:]
# Ticker 글자수가 8개 이하인 애들만 추림
codeList = []
for i in range(len(recent)):
    if len(recent['code'][i]) > 8:
        pass
    else:
        codeList.append('FRED/' + recent['code'][i])
testData = pd.read_csv('testData.csv').set_index('Date')
testData.columns = codeList
testData.index = pd.to_datetime(testData.index)
testData_M = testData.resample('M').first()
x = testData_M['FRED/AAA10Y']
y = testData_M['FRED/AAAFF']

#lag = 1
#
#coint(x,y)
#moving_coint(x,y,100)
#
#x.corr(y)
#
#start = time.time()
#winSize = 50
#x.rolling(winSize).corr(y).plot()
#moving_coint(x, y, winSize)['p_val'].plot()
#end = time.time()
#print(end-start)

start = time.time()
lag_month_range = [-18,-15,-12,-9,-6,-3,-2,-1,0,1,2,3,6,9,12,15,18]
recent_month_range = [36,60,120]
pvalList = []

for recent in recent_month_range:
    x_recent = x.iloc[-recent:,]
    y_recent = y.iloc[-recent:,]
    for lag in lag_month_range:
        print('lag : ', lag)
        print('before: ', recent)
        res = coint_lead(x_recent, y_recent,lag)
        pvalList.append((recent, lag,res[1],res[0]))#.plot()        
end = time.time()
print(end-start)


