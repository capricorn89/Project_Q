# -*- coding: utf-8 -*-
"""
Created on Tue May 14 15:53:59 2019

@author: Woojin
"""

import os
import pandas as pd
import numpy as np
import datetime
import pymysql
import xlrd
import time
import sys
from tqdm import tqdm


os.chdir('C:/Woojin/###. Git/Project_Q/I. Value and Earnings Momentum')    
import backtest_pipeline as backtest
import util
path = 'C:/Woojin/##. To-do/value_earnMom 전략/rawData/res'
os.chdir(path)

###############################################################################
# Save Backtests
###############################################################################

fileName = 'basket_190524.xlsx'
sheetNames = xlrd.open_workbook(fileName, on_demand = True).sheet_names()
print(sheetNames)

rebal_1 = pd.read_excel(fileName, sheet_name = 'addKOSPI')[['date','code', 'weight']] # 리밸런싱 스케쥴 로드
rebal_2 = pd.read_excel(fileName, sheet_name = 'replacedwithKOSPI')[['date','code', 'weight']] # 리밸런싱 스케쥴 로드
rebal_3 = pd.read_excel(fileName, sheet_name = 'onlyK200')[['date','code', 'weight']] # 리밸런싱 스케쥴 로드
rebal_4 = pd.read_excel(fileName, sheet_name = 'addKOSDAQ')[['date','code', 'weight']] # 리밸런싱 스케쥴 로드
rebal_5 = pd.read_excel(fileName, sheet_name = 'addKOSPI_opt')[['date','code', 'weight']] # 리밸런싱 스케쥴 로드
rebal_6 = pd.read_excel(fileName, sheet_name = 'replacedwithKOSPI_opt')[['date','code', 'weight']] # 리밸런싱 스케쥴 로드
rebal_7 = pd.read_excel(fileName, sheet_name = 'onlyK200_opt')[['date','code', 'weight']] # 리밸런싱 스케쥴 로드
rebal_8 = pd.read_excel(fileName, sheet_name = 'addKOSDAQ_opt')[['date','code', 'weight']] # 리밸런싱 스케쥴 로드


dollar = 1000  # Dollar invested

port_1, tc_1, to_1 = backtest.get_backtest_history(dollar, rebal_1, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 
port_2, tc_2, to_2 = backtest.get_backtest_history(dollar, rebal_2, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 
port_3, tc_3, to_3 = backtest.get_backtest_history(dollar, rebal_3, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 
port_4, tc_4, to_4 = backtest.get_backtest_history(dollar, rebal_4, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 
port_5, tc_5, to_5 = backtest.get_backtest_history(dollar, rebal_5, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 
port_6, tc_6, to_6 = backtest.get_backtest_history(dollar, rebal_6, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 
port_7, tc_7, to_7 = backtest.get_backtest_history(dollar, rebal_7, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 
port_8, tc_8, to_8 = backtest.get_backtest_history(dollar, rebal_8, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 

ports = [port_1, port_2, port_3, port_4, port_5, port_6, port_7, port_8]
for i in range(len(ports)):
    ports[i].columns = [sheetNames[i]]

turnover = [to_1, to_2, to_3, to_4, to_5, to_6, to_7, to_8]
for i in range(len(turnover)):    
    turnover[i] = pd.DataFrame(list(turnover[i].values()), index = list(turnover[i].keys()))
    turnover[i].columns = [sheetNames[i]]
turnover = pd.concat(turnover, axis = 1)


bm = util.get_index_price(['I.001','I.101'], port_1.index[0], port_1.index[-1])/100
data = pd.concat([port_1, port_2, port_3, port_4, port_5, port_6, port_7, port_8 , bm], axis= 1)

turnover.to_excel('turnover_190524.xlsx')
data.to_excel('result_190524_70bp.xlsx')


###############################################################################
# Load Backtests
###############################################################################

data = pd.read_excel('result_190524_70bp.xlsx', sheet_name = 'raw')
data = data.iloc[:, :9]

def cagr(bb, eb, n):
    return ( (eb/bb) ** (1/n) ) - 1

def sharpe_annual(priceSeries):  
    return (priceSeries.pct_change().mean() / priceSeries.pct_change().std() )  * np.sqrt(252)
   

# Print CAGR, Sharpe ratio
    
for i in range(len(data.columns)):
    
    print(data.columns[i])
    end = data.iloc[-1, i]
    start = data.iloc[1, i]
    n = 17 + (1/3)
    print("CAGR : ", "{:.2f}".format(cagr(start, end, n) * 100), "%")
    print('\n')
    
sharpe = data.resample('Y').apply(sharpe_annual)    
sharpe.to_excel('sharpe_190522.xlsx')

# Turnover
    
def weightHistory(rebalData):
    rebalData = rebalData[rebalData['weight'] > 1e-6]
    pf = {}    
    for i, _ in rebalData.groupby('date'):
        pf[i] = _[['code','weight']].set_index('code')    

    for i in range(len(pf)):
        if i == 0:
            pf_monthly =  pf[list(pf.keys())[0]]
            pf_monthly.columns = [list(pf.keys())[0]]            
        elif i > 0 :            
            pf_next = pf[list(pf.keys())[i]]
            pf_next.columns = [list(pf.keys())[i]]
            pf_monthly = pd.merge(pf_monthly, pf_next, how='outer', left_on = pf_monthly.index, right_on = pf_next.index).set_index('key_0')
        
    return pf_monthly

portHistory_6 = weightHistory(rebal_6)
portHistory_6.to_excel('portHistory_6.xlsx')



