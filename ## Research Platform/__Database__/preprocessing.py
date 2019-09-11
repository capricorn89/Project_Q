# -*- coding: utf-8 -*-
"""
Created on Wed Jan 23 18:37:45 2019

@author: Woojin Ji
"""


#############################################################################
# Time Series Matching
#############################################################################

import pandas as pd

# 데일리 데이터의 월초 데이터를 월간 데이터와 맞추고 싶을때
def mergeData(series_1, series_2, frequency='M', first_or_last='first'):
        
    series_1=series_1.resample(frequency).first()
    series_2=series_2.resample(frequency).first()
    res = pd.concat([series_1, series_2],axis=1).dropna()

    return res


def rebase(series_, start=100):

    series_return = series_.pct_change()
    series_return.iloc[0,:] = 0
    series_cumreturn = (series_return+1).cumprod()
    series_cumreturn = series_cumreturn*start    
    
    return series_cumreturn




