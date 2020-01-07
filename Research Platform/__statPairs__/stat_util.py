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





