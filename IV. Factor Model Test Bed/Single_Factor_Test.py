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
from scipy import stats
import backtest_pipeline as bt
import util

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
        

def get_priceRatio(mktcapData, factorData):      
  
    code_f = factorData.index.values
    code_p = mktcapData.columns.values
    code = list(set(code_f).intersection(code_p))
    
    ratioData = pd.DataFrame(index = code)
    ratioData['factor'] = factorData[code].values
    ratioData['price'] = mktcapData[code].values[0]
    
    ratioData = ratioData[ratioData['factor'] != 0]
        
    try:    
        ratioData['ratio'] = ratioData['price'].divide(ratioData['factor'])
        #print(ratioData)
    except ZeroDivisionError:
        print('division Error')
    
    return ratioData['ratio']
 
def using_mstats(s):
    return stats.mstats.winsorize(s, limits=[0.05, 0.05])

def winsorize_df(df):
    return df.apply(using_mstats, axis=0)




def get_longshort(ratioData, num_group = 10, asc = True, sector_on = False, allGroup = False):
    '''
    priceRatio 데이터를 인풋으로 받아 랭킹별로 그룹핑하여 출력
    - num_group : 그룹 갯수
    - ascending : True인 경우 0번 그룹 Long / N번 그룹 Short. False는 반대
    - sector_on : 섹터 정보까지 들어가있는 경우. 섹터 내에서 롱숏 그룹 (아직 구현안되어있음)
    - allGroup : False인 경우 맨위, 맨 아래 그룹만 출력. True인 경우 전체 그룹 다 출력
    '''
    
    '''ratio데이터 Winsorize'''
    #ratioData = ratioData[ratioData>0]
    ratioData = pd.DataFrame(ratioData)
    ratioData = winsorize_df(ratioData)  # Winsorize
    
    if sector_on == True:
        pass
    
    else:
        if allGroup == False:
            ratioData['group'] = pd.qcut(ratioData.values.squeeze(), q = num_group, labels = False)
            
            if asc == True: # ratio가 작은 그룹을 Long
                
                group_long = ratioData[ratioData['group'] == 0]
                group_short = ratioData[ratioData['group'] == num_group-1]
            
            else:  # ratio가 큰 그룹을 Long
                group_long = ratioData[ratioData['group'] == num_group-1]
                group_short = ratioData[ratioData['group'] == 0]      
        
            return group_long.index.values, group_short.index.values
        
        else:   # qcut의 모든 그룹을 다 담음
            ratioData['group'] = pd.qcut(ratioData.values.squeeze(), q = num_group, labels = False)
            group_n = {}
            if asc == True: 
                for group in range(num_group):                    
                    group_n[group] = ratioData[ratioData['group'] == group]
                
            else:
                for group in range(num_group):
                    group_n[group] = ratioData[ratioData['group'] == num_group-group]
            
            return group_n


def get_priceMom(codes, rebalDate):
    
    yearAgo = rebalDate - datetime.timedelta(days=365)
    monthAgo = rebalDate - datetime.timedelta(days=30)
    
    yearAgo = util.get_recentBday(yearAgo)
    monthAgo = util.get_recentBday(monthAgo)
    rebalDate = util.get_recentBday(rebalDate)
    
    prices = util.get_stock_price(codes, yearAgo, rebalDate)
    
    mom_12M = (prices.loc[rebalDate, :] / prices.loc[yearAgo, :]) - 1
    mom_1M = (prices.loc[rebalDate, :] / prices.loc[monthAgo, :]) - 1
    
    momData = (mom_12M - mom_1M).dropna()
    
    return momData                    
        
def to_backtestFormat(long_test, rebalDate):

    longData = pd.DataFrame(index = range(len(long_test)))   
    longData['date'] = rebalDate    
    longData['code'] = long_test    
    longData['weight'] = np.ones(len(long_test)) / len(long_test)

    return longData[['date','code','weight']]    
            
        
def get_portfolio(factor, factorStyle = 'ratio'):

    rebalData_long = []
    rebalData_short = []
    
    for i in range(len(rebal_sche)):
        
        rebalDate_ = util.get_recentBday(rebal_sche[i], dateFormat = 'datetime')
        #print(rebalDate_)
        
        univ_ = getUniverse(marketInfo, mktcap, risk_1, risk_2, rebalDate_)
        print(len(univ_))
           
        if factorStyle == 'ratio':
            
            factor = factor.loc[:,univ_]  
            factor_ = getFactorData(factor, rebalDate_)
            mktcap_ = util.get_mktcap(factor_.index.values, rebalDate_, rebalDate_)
            ratio_ = get_priceRatio(mktcap_, factor_)
            long, short = get_longshort(ratio_, num_group = 10)
            longFinal = to_backtestFormat(long, rebalDate_)
            shortFinal = to_backtestFormat(short, rebalDate_)            
            rebalData_long.append(longFinal)
            rebalData_short.append(shortFinal)   
            
        elif factorStyle == 'Momentum':
            
            factor_mom = get_priceMom(univ_, rebalDate_)
            long, short = get_longshort(factor_mom, num_group = 10, asc = False)
            longFinal = to_backtestFormat(long, rebalDate_)
            shortFinal = to_backtestFormat(short, rebalDate_)            
            rebalData_long.append(longFinal)
            rebalData_short.append(shortFinal)              
    
    rebalData_long = pd.concat(rebalData_long).reset_index()[['date','code','weight']]
    rebalData_short = pd.concat(rebalData_short).reset_index()[['date','code','weight']]
    
    return rebalData_long, rebalData_short


os.chdir('C:/Woojin/###. Git/Project_Q/IV. Factor Model Test Bed')

start_year, start_month = 2006, 12
start_day = calendar.monthrange(start_year, start_month)[1]
end_year, end_month = 2019, 3
end_day = calendar.monthrange(end_year, end_month)[1]

start_invest = pd.datetime(start_year, start_month, start_day)
end_invest = pd.datetime(end_year, end_month, end_day)
rebal_sche = pd.date_range(start_invest, end_invest, freq = 'M')

marketInfo = util.data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'market'))
mktcap = util.data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'mktcap'))
risk_1 = util.data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'risk_1'))
risk_2 = util.data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'risk_2'))

factor_PSR = util.data_cleansing(pd.read_excel('testData.xlsx', sheet_name = 'sales'))
factor_PBR = util.data_cleansing(pd.read_excel('testData.xlsx', sheet_name = 'book'))
factor_PER = util.data_cleansing(pd.read_excel('testData.xlsx', sheet_name = 'earnings'))

long_PSR, short_PSR = get_portfolio(factor_PSR)
long_PBR, short_PBR = get_portfolio(factor_PBR)
long_PER, short_PER = get_portfolio(factor_PER)
long_MOM, short_MOM = get_portfolio(None, factorStyle = 'Momentum')

##############################################################

res_long = bt.get_backtest_history(1000, long_MOM)
res_short = bt.get_backtest_history(1000, short_MOM)

results_l = res_long[0]
results_s = res_short[0]

result = pd.concat([results_l, results_s], axis  = 1)
result.columns = ['long', 'short']
result['longShort_return'] = np.subtract(result.pct_change()['long'],result.pct_change()['short'])

bm = util.get_index_price(['I.001','I.101'], results_l.index[0], results_l.index[-1])/100

result = pd.concat([result, bm], axis = 1)

result.to_excel('backtest_result_MOM.xlsx')

(result['longShort_return']+ 1).cumprod().plot()





