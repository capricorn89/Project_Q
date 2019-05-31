# -*- coding: utf-8 -*-
"""
Created on Fri May 24 08:20:52 2019

@author: woojin
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
from scipy import stats
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

def get_stock_price(stockCodes, date_start, date_end):
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
    sql = "SELECT GICODE, TRD_DT, ADJ_PRC FROM dg_fns_jd WHERE TRD_DT BETWEEN " + date_start
    sql += ' AND ' + date_end
    sql += (" AND GICODE IN (\'" + joined + "\')")
    cursor.execute(sql)
    data = cursor.fetchall()
    data = pd.DataFrame(list(data))
    data = data.pivot(index = 1, columns = 0, values = 2)
    data.index = pd.to_datetime(data.index.values)
    db.close()   
    return data

def get_mktcap(stockCodes, date_start, date_end):
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
    sql = "SELECT GICODE, TRD_DT, (ADJ_PRC * LIST_STK_CNT)/100000000 MKTCAP FROM dg_fns_jd WHERE TRD_DT BETWEEN " + date_start
    sql += ' AND ' + date_end
    sql += (" AND GICODE IN (\'" + joined + "\')")
    cursor.execute(sql)
    data = cursor.fetchall()
    data = pd.DataFrame(list(data))
    data = data.pivot(index = 1, columns = 0, values = 2)
    data.index = pd.to_datetime(data.index.values)
    db.close()   
    return data


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

def get_amt_money(weightList, totalMoney):
    '''
    weightList : array
    totalMoney : scalar
    '''    
    return weightList * totalMoney

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


def get_num_stock(moneyList, priceList):
    '''
    moneyList : array
    priceList : array
    '''    
    return moneyList / priceList


def get_basket_history(stockCodes, numStock, date_start, date_end):
    basketPriceData =  get_stock_price(stockCodes, date_start, date_end).fillna(0)
    dates = basketPriceData.index
    priceHistory = basketPriceData[stockCodes].values.dot(numStock)    
    priceHistory = pd.DataFrame(priceHistory, index = dates)
    return priceHistory
    
def get_equalweight(stockCode):   
    return np.ones(len(stockCode)) / len(stockCode)


def getFinancialData(factorData, rebalDate, dataPeriod='Q'):

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
    data = factorData.loc[:date_available,:].iloc[-1,:].dropna()

    return data

def getFinancialData_TTM(factorData, rebalDate, dataPeriod='Q'):

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
    data = factorData.loc[:date_available,:].iloc[-4:,:].sum().dropna()

    return data


def getUniverse(marketInfoData, mktcapData, riskInfo_1, riskInfo_2, rebalDate_, 
                universeName = 'KOSPI', mktcapLimit = 2000):
    '''market에서 KOSPI인 종목 중에서
    거래정지, 관리종목인 종목 거르고,
    시가총액 2000억 미만 거르고, 
    20일 평균거래대금 10억 이상으로 필터링'''
    
    # 상장시장
    m = marketInfoData.loc[rebalDate_, :]
    inMarket = m[m == universeName].index.values
    
    # 거래정지
    notRisk_1 = riskInfo_1.loc[rebalDate_, :]
    notRisk_1 = notRisk_1[notRisk_1 == 0].index.values
    
    #관리종목
    notRisk_2 = riskInfo_2.loc[rebalDate_, :]
    notRisk_2 = notRisk_2[notRisk_2 == 0].index.values
    
#    # 거래대금 기준
#    v = volInfo.loc[rebalDate_, :]
#    above10 = v[v >= tradeVolLimit].index.values
#    
    # 시총기준
    cap = mktcapData.loc[rebalDate_, :]
    cap = cap[cap >= mktcapLimit].index.values
           
    res = set(set(set(inMarket).intersection(notRisk_1)).intersection(notRisk_2)).intersection(cap)
    
    return list(res)


def using_mstats(s):
    return stats.mstats.winsorize(s, limits=[0.025, 0.025])

def winsorize_df(df):
    return df.apply(using_mstats, axis=0)

def get_priceMom(codes, rebalDate):
    
    yearAgo = rebalDate - datetime.timedelta(days=365)
    monthAgo = rebalDate - datetime.timedelta(days=30)
    
    yearAgo = get_recentBday(yearAgo)
    monthAgo = get_recentBday(monthAgo)
    rebalDate = get_recentBday(rebalDate)
    
    prices = get_stock_price(codes, yearAgo, rebalDate)
    
    mom_12M = (prices.loc[rebalDate, :] / prices.loc[yearAgo, :]) - 1
    mom_1M = (prices.loc[rebalDate, :] / prices.loc[monthAgo, :]) - 1
    
    momData = (mom_12M - mom_1M).dropna()
    
    return momData   

def get_adjMom(codes, rebalDate):
    
    yearAgo = rebalDate - datetime.timedelta(days=365)
    monthAgo = rebalDate - datetime.timedelta(days=30)
    
    yearAgo = get_recentBday(yearAgo)
    monthAgo = get_recentBday(monthAgo)
    rebalDate = get_recentBday(rebalDate)
    
    prices = get_stock_price(codes, yearAgo, rebalDate)
    
    mom_12M = (prices.loc[rebalDate, :] / prices.loc[yearAgo, :]) - 1
    mom_1M = (prices.loc[rebalDate, :] / prices.loc[monthAgo, :]) - 1
    
    momData = mom_12M - mom_1M
    returnVOL = prices.pct_change().std()
    momData = (momData / returnVOL).dropna()
    
    return momData


def get_inverseVol(codes, rebalDate):
    '''1년간 일별 수익률 표준편차의 역수'''
    
    yearAgo = rebalDate - datetime.timedelta(days=365)
    yearAgo = get_recentBday(yearAgo)
    rebalDate = get_recentBday(rebalDate)
    
    prices = get_stock_price(codes, yearAgo, rebalDate).pct_change()
    inverseVol = 1 / prices.std()
    
    return inverseVol 

def to_portfolio(codes, rebalDate, weight = 'equal'):       
    df = pd.DataFrame(index = range(len(codes)), columns = ['date','code', 'weight'])
    df['code'] = codes
    df['weight'] = np.ones(len(codes)) / len(codes)
    df['date'] = rebalDate
    return df

def to_zscore(factor):
    return (factor - factor.mean()) / factor.std()


def get_multifactor_score(factor_df):
    factor_df = to_zscore(factor_df)
    factor_df['score'] = factor_df.sum(axis=1)
    return factor_df['score']



####################################
#            Visualize  
####################################
#'''
#import matplotlib.pyplot as plt
#import matplotlib.dates as mdates
#import numpy as np
#plt.style.use('fivethirtyeight')
#
#date = cum_returnData.index.astype('O')
#close = cum_returnData[['BM', 'Fund', 'Fund_PBR']]
#fig, ax = plt.subplots(figsize = (10,8))
#fig.autofmt_xdate()
#ax.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
#ax.plot(date, close, lw=2)
#plt.show()
#
## create two subplots with the shared x and y axes
#fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, sharey=False, figsize = (10, 8))
#ax1.plot(date, cum_returnData.BM, lw = 2, label = 'BM(KOSPI SmallCap)')
#ax1.plot(date, cum_returnData.Fund, lw = 2, label = 'Fund')
#ax2.fill_between(date, 0, cum_returnData['ER'], label = 'Excess Return', facecolor = 'blue', alpha = 0.5)
#for ax in ax1, ax2:
#    ax.grid(True)
#    ax.legend(fancybox=True, framealpha = 0.5, loc = 2)
#    
#fig.autofmt_xdate()
#plt.show()
#
#
