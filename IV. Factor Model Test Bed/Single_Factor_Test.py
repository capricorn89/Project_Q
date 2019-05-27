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
    data = factorData.loc[:date_available,:].iloc[-1,:].dropna()

    return data
        
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




def get_priceRatio(priceData, factorData, factorName):      
  
    code_f = factorData.index.values
    code_p = priceData.columns.values
    code = list(set(code_f).intersection(code_p))
    
    ratioData = pd.DataFrame(index = code)
    ratioData[factorName] = factorData[code].values
    ratioData['price'] = priceData[code].values[0]
    
    ratioData = ratioData[ratioData[factorName] != 0]
        
    try:    
        ratioData['ratio'] = ratioData['price'].divide(ratioData[factorName])
        #print(ratioData)
    except ZeroDivisionError:
        print('division Error')
    
    return ratioData['ratio']



def get_longshort(ratioData, num_group = 10, ascending = True, sector_on = False, allGroup = False):
    
    ratioData = ratioData[ratioData>0]
    ratioData = pd.DataFrame(ratioData)
    
    '''ratio를 z-score로 전환후 랭킹 출력'''
    
    if sector_on == True:
        pass
    
    else:
        if allGroup == False:
            ratioData['group'] = pd.qcut(ratioData.values.squeeze(), q = num_group, labels = False)
            
            if ascending == True: # ratio가 작은 그룹을 Long
                
                group_long = ratioData[ratioData['group'] == 0]
                group_short = ratioData[ratioData['group'] == num_group-1]
            
            else:
                group_long = ratioData[ratioData['group'] == num_group-1]
                group_short = ratioData[ratioData['group'] == 0]      
        
            return group_long.index.values, group_short.index.values
        
        else:   # qcut의 모든 그룹을 다 담음
            ratioData['group'] = pd.qcut(ratioData.values.squeeze(), q = num_group, labels = False)
            group_n = {}
            if ascending == True: 
                for group in range(num_group):                    
                    group_n[group] = ratioData[ratioData['group'] == group]
                
            else:
                for group in range(num_group):
                    group_n[group] = ratioData[ratioData['group'] == num_group-group]
            
            return group_n
                    
        


        
def to_backtestFormat(long_test, rebalDate):

    longData = pd.DataFrame(index = range(len(long_test)))   
    longData['date'] = rebalDate    
    longData['code'] = long_test    
    longData['weight'] = np.ones(len(long_test)) / len(long_test)

    return longData[['date','code','weight']]    
            
            
    
os.chdir('C:/Woojin/###. Git/Project_Q/IV. Factor Model Test Bed')

start_year = 2006
start_month = 12
start_day = calendar.monthrange(start_year, start_month)[1]

end_year = 2019
end_month = 3
end_day = calendar.monthrange(end_year, end_month)[1]

start_invest = pd.datetime(start_year, start_month, start_day)
end_invest = pd.datetime(end_year, end_month, end_day)
rebal_sche = pd.date_range(start_invest, end_invest, freq = 'M')

factor = data_cleansing(pd.read_excel('testData.xlsx', sheet_name = 'sales'))
marketInfo = data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'market'))
mktcap = data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'mktcap'))
risk_1 = data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'risk_1'))
risk_2 = data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'risk_2'))


rebalData_long = []
rebalData_short = []

for i in range(len(rebal_sche)):
    
    rebalDate_ = get_recentBday(rebal_sche[i], dateFormat = 'datetime')
    print(rebalDate_)
    factor_ = getFactorData(factor, rebalDate_)
    univ_ = getUniverse(marketInfo, mktcap, risk_1, risk_2, rebalDate_)
    price_ = get_stock_price(factor_.index.values, rebalDate_, rebalDate_)
    ratio_ = get_priceRatio(price_, factor_, 'sales')
    long, short = get_longshort(ratio_, 10)
    longFinal = to_backtestFormat(long, rebalDate_)
    shortFinal = to_backtestFormat(short, rebalDate_)
    
    rebalData_long.append(longFinal)
    rebalData_short.append(shortFinal)
   

rebalData_long = pd.concat(rebalData_long).reset_index()[['date','code','weight']]
rebalData_short = pd.concat(rebalData_short).reset_index()[['date','code','weight']]

rebalData_long.to_excel('backTest_PSR_long.xlsx')
rebalData_short.to_excel('backTest_PSR_short.xlsx')



import backtest_pipeline as bt
import util

res_long = bt.get_backtest_history(1000, rebalData_long)
res_short = bt.get_backtest_history(1000, rebalData_short)

results_l = res_long[0]
results_s = res_short[0]

result = pd.concat([results_l, results_s], axis  = 1)
result.columns = ['long', 'short']
result['longShort_return'] = np.subtract(result.pct_change()['long'],result.pct_change()['short'])

bm = util.get_index_price(['I.001','I.101'], results_l.index[0], results_l.index[-1])/100

result = pd.concat([result, bm], axis = 1)

result.to_excel('backtest_result.xlsx')

(result['longShort_return']+ 1).cumprod().plot()





