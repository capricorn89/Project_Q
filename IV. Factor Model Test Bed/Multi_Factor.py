# -*- coding: utf-8 -*-
"""
Created on Tue May 28 15:19:57 2019

@author: Woojin

< Multi-factor Model >

"""

import pandas as pd
import os
import numpy as np
import datetime
import pymysql
from tqdm import tqdm
import calendar
from scipy import stats

os.chdir('C:/Woojin/###. Git/Project_Q/IV. Factor Model Test Bed')

import backtest_pipeline as bt
import util


start_year, start_month = 2006, 12
start_day = calendar.monthrange(start_year, start_month)[1]
end_year, end_month = 2019, 3
end_day = calendar.monthrange(end_year, end_month)[1]

start_invest = pd.datetime(start_year, start_month, start_day)
end_invest = pd.datetime(end_year, end_month, end_day)
rebal_sche = pd.date_range(start_invest, end_invest, freq = 'M')

# Load market info data
marketInfo = util.data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'market'))
mktcap = util.data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'mktcap'))
risk_1 = util.data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'risk_1'))
risk_2 = util.data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'risk_2'))

# Load factor data
factor_PSR = util.data_cleansing(pd.read_excel('testData.xlsx', sheet_name = 'sales'))
factor_PBR = util.data_cleansing(pd.read_excel('testData.xlsx', sheet_name = 'book'))
factor_PER = util.data_cleansing(pd.read_excel('testData.xlsx', sheet_name = 'earnings'))

# I. Cross-sectional

''' 
ex. Value & Momentum (Sales to Price, Book to Price, Momentum)
'''

def get_priceRatio_multi(factorData, mktcapData):
    code_f = factorData.index.values
    code_p = mktcapData.columns.values
    code = list(set(code_f).intersection(code_p))
    #print(code)
    ratioData = pd.DataFrame(index = code, columns = factorData.columns)
    ratioData[factorData.columns] = factorData.loc[code,:].values    
    ratioData['mktcap'] = mktcapData[code].values[0]    
    
    for factors in factorData.columns:
        ratioData[factors] = ratioData[factors] / ratioData['mktcap'] 
    
    return ratioData[factorData.columns].dropna(how='all')

def to_zscore(factor):
    return (factor - factor.mean()) / factor.std()

def get_multifactor_score(factor_df):
    factor_df = to_zscore(factor_df)
    factor_df['score'] = factor_df.sum(axis=1)
    return factor_df['score']

def to_portfolio(codes, rebalDate, weight = 'equal'):       
    df = pd.DataFrame(index = range(len(codes)), columns = ['date','code', 'weight'])
    df['code'] = codes
    df['weight'] = np.ones(len(codes)) / len(codes)
    df['date'] = rebalDate
    return df


rebalData_long = []
rebalData_short = []
num_group = 5
method = 'integrated'


for i in tqdm(range(len(rebal_sche))):

    date_spot = util.get_recentBday(rebal_sche[i], dateFormat = 'datetime')
    univ_ = util.getUniverse(marketInfo, mktcap, risk_1, risk_2, date_spot)
    psr_spot = util.getFinancialData(factor_PSR, date_spot)[univ_]
    pbr_spot = util.getFinancialData(factor_PBR, date_spot)[univ_]
   
    # I-1. Integrated Method (Signal Blend, 개별 팩터 스코어를 모두 더해 한번에 주식을 뽑는 방법)
    if method == 'integrated':

        factorName = ['sales','book']
#        factorName = ['book']
        multifactor_df = pd.concat([psr_spot, pbr_spot], axis = 1, sort = False)
#        multifactor_df = pd.concat([pbr_spot], axis = 1, sort = False)
        multifactor_df.columns = factorName
        mktcaps = util.get_mktcap(multifactor_df.index.values, date_spot, date_spot)
        
        port = get_priceRatio_multi(multifactor_df, mktcaps)
        mom_spot = util.get_priceMom(univ_, date_spot)  # Add momentum factor
        port = pd.concat([port, mom_spot], axis = 1, sort=False)
        port = to_zscore(port) # trasnform to Z-score  
        
        port = get_multifactor_score(port)
        port = pd.qcut(port, num_group, labels =False)
        port_long  = to_portfolio(port[port  == num_group - 1].index.values, date_spot)  # Long : Highest 20% having total z-score
        port_short = to_portfolio(port[port == 0].index.values, date_spot)
    
        rebalData_long.append(port_long)
        rebalData_short.append(port_short)

    # I-2. Composite Method (개별 팩터별로 익스포저가 높은 종목을 뽑은 뒤 합치는 방법)        
    elif method == 'composite':
        pass
    # I-3. Priority Method (1번 팩터로 거른 뒤 , 2번 팩터로 거르는 식으로 순차적으로 종목을 필터링하는 방법)    
    elif method == 'priority':
        pass
     
    else:
        pass

rebalData_long = pd.concat(rebalData_long).reset_index()[['date','code','weight']]
rebalData_short = pd.concat(rebalData_short).reset_index()[['date','code','weight']]
print('Portfolio completed')

#rebalData_long.to_excel('multifator_basket_long_' + method + '.xlsx')
#rebalData_long.to_excel('multifator_basket_short_' + method + '.xlsx')

result_long = bt.get_backtest_history(1000, rebalData_long)[0]
result_short = bt.get_backtest_history(1000, rebalData_short)[0]
result = pd.concat([result_long, result_short], axis = 1)
result.columns = ['long', 'short']
result['longShort_return'] = np.subtract(result.pct_change()['long'],result.pct_change()['short'])
bm = util.get_index_price(['I.001','I.101'], result_long.index[0], result_long.index[-1])/100
result = pd.concat([result, bm], axis = 1)
result['I.101_return'] = result['I.101'].pct_change()
(result[['longShort_return', 'I.101_return']] + 1).cumprod().plot()
result.to_excel('multifactor.xlsx')




# II. Time Series 

