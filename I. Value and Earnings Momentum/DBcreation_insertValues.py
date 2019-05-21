# -*- coding: utf-8 -*-
"""
Created on Tue May 21 10:26:58 2019

@author: Woojin
"""

path = 'C:/Woojin/##. To-do/value_earnMom 전략/rawData'

import os
import pandas as pd
import numpy as np
import datetime
import calendar
import pymysql

os.chdir(path)
def data_cleansing(rawData):
    
    firmCode = rawData.iloc[7,5:].values
    yearIndex = [int(str(x)[:4]) for x in rawData.iloc[10:,1].values]
    monthIndex = [int(str(x)[4:]) for x in rawData.iloc[10:,1].values]
    newDateIndex = []
    for i in range(len(yearIndex)):
        days = calendar.monthrange(yearIndex[i], monthIndex[i])[1]
        newDateIndex.append(datetime.datetime(yearIndex[i], monthIndex[i], days))
    
    newData = rawData.iloc[10:,5:]
    newData.columns = firmCode
    newData.index = newDateIndex
    
    return newData

def data_cleansing_ts(rawData):
    
    firmCode = rawData.iloc[6, 1:].values
    dateIndex = rawData.iloc[13:, 0].values
    newData = rawData.iloc[13:,1:]
    newData.columns = firmCode
    newData.index = dateIndex
    return newData



# 재무정보(quantData_Q 테이블 업데이트)
ocf = data_cleansing(pd.read_excel('fin.xlsx', sheet_name = 'ocf_Q'))
cfTTM = data_cleansing(pd.read_excel('fin.xlsx', sheet_name = 'cfTTM_Q'))
ocfTTM = data_cleansing(pd.read_excel('fin.xlsx', sheet_name = 'ocfTTM_Q'))
opmTTM = data_cleansing(pd.read_excel('fin.xlsx', sheet_name = 'opm_Q'))

ocf = ocf.unstack(level=1).reset_index()             
ocf.columns = ['code', 'date', 'ocf']
cfTTM = cfTTM.unstack(level=1).reset_index()                
cfTTM.columns = ['code', 'date', 'cf_TTM']
ocfTTM = ocfTTM.unstack(level=1).reset_index()             
ocfTTM.columns = ['code', 'date', 'ocf_TTM']
opmTTM = opmTTM.unstack(level=1).reset_index()                
opmTTM.columns = ['code', 'date', 'opm_TTM']



quantData_Q = pd.merge(ocf, cfTTM, how = 'outer' , left_on = ['code' , 'date'], right_on = ['code' , 'date'])
quantData_Q = pd.merge(quantData_Q, ocfTTM, how = 'outer' , left_on = ['code' , 'date'], right_on = ['code' , 'date'] )
quantData_Q = pd.merge(quantData_Q, opmTTM, how = 'outer' , left_on = ['code' , 'date'], right_on = ['code' , 'date'] )
quantData_Q = quantData_Q[['code', 'date', 'ocf', 'ocf_TTM', 'cf_TTM', 'opm_TTM']]

from sqlalchemy import create_engine
engine = create_engine('sqlite:////C:/Woojin/###. Git/Project_Q/I. Value and Earnings Momentum/rdata.db', echo=False)

quantData_Q.to_sql('quantData_Q', con=engine)
engine.excute("SELECT * FROM quantData_Q").fetchall()





# 수급 및 유동성 정보 (20일 거래대금, 20일누적 기관순매수수량, 시가총액, 상장주식수)
vol_20MA = data_cleansing_ts(pd.read_excel('liq.xlsx', sheet_name = 'vol_20MA_M'))
netbuy20 = data_cleansing_ts(pd.read_excel('liq.xlsx', sheet_name = 'netbuy20_M'))
mktcap = data_cleansing_ts(pd.read_excel('liq.xlsx', sheet_name = 'mktcap_M'))
numStock = data_cleansing_ts(pd.read_excel('liq.xlsx', sheet_name = 'numStock_M'))


# 상장시장, 섹터, 거래정지 여부 등 기본 정보
market = data_cleansing_ts(pd.read_excel('info.xlsx', sheet_name ='market_M'))
sector = data_cleansing_ts(pd.read_excel('info.xlsx', sheet_name ='sector_M'))
risk_1 = data_cleansing_ts(pd.read_excel('info.xlsx', sheet_name ='risk_1_M'))
risk_2 = data_cleansing_ts(pd.read_excel('info.xlsx', sheet_name ='risk_2_M'))
inK200 = data_cleansing_ts(pd.read_excel('info.xlsx', sheet_name ='inK200_M'))
inKQ150 = data_cleansing_ts(pd.read_excel('info.xlsx', sheet_name ='inKQ150_M'))
