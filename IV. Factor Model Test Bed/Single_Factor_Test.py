# -*- coding: utf-8 -*-
"""
Created on Thu May 23 09:22:58 2019

@author: Woojin
"""

import pandas as pd
import os
import numpy as np
import datetime
import pymysql
from tqdm import tqdm
import calendar


def data_cleansing(rawData):
    '''Quantiwise 제공 재무데이터 클렌징 용도
    YYYYMM 형태로 데이터가 나오기 때문에 yyyy-mm-dd 로 변경 (dd는 말일 날짜)
    '''
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
    '''Quantiwise 제공 시계열데이터 클렌징 용도
    - 열 : 종목명
    - 행 : 시계열
    '''    
    firmCode = rawData.iloc[6, 1:].values
    dateIndex = rawData.iloc[13:, 0].values
    newData = rawData.iloc[13:,1:]
    newData.columns = firmCode
    newData.index = dateIndex
    return newData

def get_recentBday(date, dateFormat = 'sql'):
    '''최근 영업일자 추출'''
    date = ''.join([x for x in str(date)[:10] if x != '-'])
    db = pymysql.connect(host='192.168.1.190', port=3306, user='root', passwd='gudwls2@', db='quant_db',charset='utf8',autocommit=True)
    cursor = db.cursor()    
    sql = "SELECT DISTINCT TRD_DT FROM dg_fns_jd WHERE TRD_DT <=" + date 
    sql += " ORDER BY TRD_DT"
    cursor.execute(sql)
    data = cursor.fetchall()
    data = data[-1][0]
    db.close()
    
    if dateFormat == 'sql':
        data = data
    elif dateFormat == 'datetime':
        data = pd.datetime(int(data[:4]),int(data[4:6]), int(data[6:]))
    else:
        pass
    
    return data




start_year = 2007
start_month = 1
start_day = calendar.monthrange(start_year, start_month)[1]

end_year = 2019
end_month = 3
end_day = calendar.monthrange(end_year, end_month)[1]

start_invest = pd.datetime(start_year, start_month, start_day)
end_invest = pd.datetime(end_year, end_month, end_day)
rebal_sche = pd.date_range(start_invest, end_invest, freq = 'M')


os.chdir('C:/Woojin/###. Git/Project_Q/IV. Factor Model Test Bed')

factor = data_cleansing(pd.read_excel('testData.xlsx', sheet_name = 'factor'))
marketInfo = data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'market'))


def getUniverse(marketInfoData, rebalDate, universeName = 'KOSPI'):
    '''market에서 유니버스 이름(ex, KOSPI)로 필터링'''    
    m = marketInfoData.loc[:rebalDate, :].iloc[-1,:]
    m = m[m == universeName]
    return m.index.values

def getFactorData(factorData, rebalDate, dataPeriod='Q'):

    ''' 분기 발표 재무데이터 사용 기준 (가용 시점이 다르기 때문)
    
    가용 최신 데이터 : 매월말 기준으로 해당분기의 직전 분기말 
    
        종목 선정 시점     |   가용데이터 인덱스  (T)        |   가용데이터 인덱스 (T-1)
    ---------------------------------------------------------------------------------
            3월말          |        12말(전년)              |       9말
            4월말          |        12말(전년)              |       9말
            5월말          |        12말(전년)              |       9말
            6월말          |         3말(당해)              |       12말
            7월말          |         3말(당해)              |       12말
            8월말          |         3말(당해)              |       12말
            9월말          |         6말(당해)              |       3말
           10월말          |         6말(당해)              |       3말
           11월말          |         6말(당해)              |       3말     
           12월말          |         9말(당해)              |       6말    
            1월말          |         9말(전년)              |       6말
            2월말          |         9말(전년)              |       6말
    '''

    if (rebalDate.month >=3) & (rebalDate.month <= 5):
        t_year = rebalDate.year - 1
        t_month = 12
        
    elif (rebalDate.month >= 6) & (rebalDate.month <= 8):
        t_year = rebalDate.year
        t_month = 3
        
    elif (rebalDate.month >= 9) & (rebalDate.month <= 11):
        t_year = rebalDate.year
        t_month = 6        

    elif rebalDate.month == 12:
        t_year = rebalDate.year
        t_month = 9    

    elif (rebalDate.month >= 1) & (rebalDate.month <= 2):
        t_year = rebalDate.year - 1
        t_month = 9    

    else:
        print('데이터 확인 필요')      

    t_day = calendar.monthrange(t_year, t_month)[1]  
    date_available = pd.datetime(t_year, t_month, t_day)
    data = factorData.loc[:date_available,:].iloc[-1,:]

    return data
        


def psr(factor, universe):
    
    
    factor = factor[universe]
        
    
    
    
    
















