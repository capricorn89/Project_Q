# -*- coding: utf-8 -*-
"""
Created on Tue May 14 15:53:59 2019

@author: user
"""

import os
import pandas as pd
import numpy as np
import datetime
import pymysql

path = 'C:/Woojin/##. To-do/value_earnMom 전략/rawData/res'
os.chdir(path)
rebal_result = pd.read_excel('basket_190515.xlsx')[['date','code', 'weight']] # 리밸런싱 스케쥴 로드
#rebal_result_old = pd.read_excel('basket_190507_sector.xlsx')[['date','code', 'weight']] # 리밸런싱 스케쥴 로드3

dollar = 1000  # Dollar invested

os.chdir('C:/Woojin/1. Codes')    
import backtest_pipeline as backtest

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


portPrice, tradeCost_history = backtest.get_backtest_history(dollar, rebal_result, equal_weight = False, roundup = False) # 백테스트 결과 출력 
portPrice_noTC, tradeCost_history_noTC = backtest.get_backtest_history(dollar, rebal_result, equal_weight = False, roundup = False, tradeCost = 0.0) # 백테스트 결과 출력 
#portPrice_old, tc_history_old =  backtest.get_backtest_history(dollar, rebal_result_old, equal_weight = False, roundup = False)
#portPrice_noTC_old, tc_history_old =  backtest.get_backtest_history(dollar, rebal_result_old, equal_weight = False, roundup = False, tradeCost = 0.0)

portPrice.columns = ['port']
portPrice_noTC.columns = ['port_noTC']
bm = get_index_price(['I.001','I.101'], portPrice.index[0], portPrice.index[-1])/100
data = pd.concat([portPrice, portPrice_noTC, bm], axis= 1)
data.to_excel('result_190515_v4.xlsx')

