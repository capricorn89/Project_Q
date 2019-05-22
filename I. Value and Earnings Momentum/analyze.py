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
path = 'C:/Woojin/##. To-do/value_earnMom 전략/rawData/res'
os.chdir(path)

###############################################################################
# Save Backtests
###############################################################################

fileName = 'basket_190522.xlsx'
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


def get_index_price(stockCodes, date_start, date_end):
    '''
    input: 
    - stockcodes : list
    - date_start : datetime
    - date_end : datetiem
    
    output : DataFrame
    - Index : Stock code
    - value : Stock Price
    '''
    date_start = ''.join([x for x in str(date_start)[:10] if x != '-'])
    date_end = ''.join([x for x in str(date_end)[:10] if x != '-'])
    db = pymysql.connect(host='192.168.1.190', port=3306, user='root', passwd='gudwls2@', db='quant_db',charset='utf8',autocommit=True)
    cursor = db.cursor()
    joined = "\',\'".join(stockCodes)
    sql = "SELECT U_CD, TRD_DT, CLS_PRC FROM dg_udsise WHERE TRD_DT BETWEEN " + date_start
    sql += ' AND ' + date_end
    sql += (" AND U_CD IN (\'" + joined + "\')")
    cursor.execute(sql)
    data = cursor.fetchall()
    data = pd.DataFrame(list(data))
    data = data.pivot(index = 1, columns = 0, values = 2)
    data.index = pd.to_datetime(data.index.values)
    db.close()   
    return data

dollar = 1000  # Dollar invested

port_1, _ = backtest.get_backtest_history(dollar, rebal_1, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 
port_2, _ = backtest.get_backtest_history(dollar, rebal_2, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 
port_3, _ = backtest.get_backtest_history(dollar, rebal_3, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 
port_4, _ = backtest.get_backtest_history(dollar, rebal_4, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 
port_5, _ = backtest.get_backtest_history(dollar, rebal_5, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 
port_6, _ = backtest.get_backtest_history(dollar, rebal_6, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 
port_7, _ = backtest.get_backtest_history(dollar, rebal_7, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 
port_8, _ = backtest.get_backtest_history(dollar, rebal_8, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 

ports = [port_1, port_2, port_3, port_4, port_5, port_6, port_7, port_8]
for i in range(len(ports)):
    ports[i].columns = [sheetNames[i]]


bm = get_index_price(['I.001','I.101'], port_1.index[0], port_1.index[-1])/100
data = pd.concat([port_1, port_2, port_3, port_4, port_5, port_6, port_7, port_8 , bm], axis= 1)
data.to_excel('result_190522_70bp.xlsx')


rebal_old = pd.read_excel('basket_190507_v2.xlsx')
port_old, _ = backtest.get_backtest_history(dollar, rebal_old, equal_weight = False, roundup = False, tradeCost = 0.007) # 백테스트 결과 출력 
port_old.to_excel('result_190507_v2.xlsx')

###############################################################################
# Load Backtests
###############################################################################

data = pd.read_excel('result_190517.xlsx', sheet_name = '70bp')
data = data.iloc[:, :8]

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
    print("Sharpe Ratio : ", "{:.4f}".format(sharpe_annual(data.iloc[1:, i])))
    print('\n')
    

    








