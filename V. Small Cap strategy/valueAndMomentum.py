# -*- coding: utf-8 -*-
"""
Created on Tue Jun 25 08:27:21 2019

@author: Woojin Ji
"""

'''
< 스몰캡 초과 수익 전략 >

목표 : 중소형주 지수 아웃퍼폼하는 코스피 시총 200위 이하 중소형주 포트폴리오 만들기

제약조건 :
    1) 시가총액 제한 : 800억 이하 기업 제외
    2) 연속 적자 : 3년 연속적자 기업 제외
    3) 거래대금 : 반기 (120일) 기준 3억 미만 기업 제외

매월 말 리밸런싱
    - 매월 말 유니버스 편성 시 시총, 거래대금으로 유니버스 재설정
    - 단, 3년 연속 적자의 경우 매년 4월 말에만 한번씩 확인

종목 선정 기준
    - 스타일 팩터
    - Crash / Jackpot
    - 

'''

import os
import pandas as pd
import numpy as np
import datetime
path = 'C:/Woojin/###. Git/Project_Q/V. Small Cap strategy'
os.chdir(path)
import util as ut

rdata = pd.ExcelFile(path + '/Data/data.xlsx')

mkt_info = ut.data_cleansing(pd.read_excel(rdata, 'market_info'))
mktcap = ut.data_cleansing(pd.read_excel(rdata, 'mktcap'))
net_income = ut.data_cleansing_as(pd.read_excel(rdata, 'net_income'))
vol = ut.data_cleansing(pd.read_excel(rdata, 'volume'))
caution_1 = ut.data_cleansing(pd.read_excel(rdata, '거래정지'))
caution_2 = ut.data_cleansing(pd.read_excel(rdata, '투자유의'))
caution_3 = ut.data_cleansing(pd.read_excel(rdata, '관리종목'))

per = ut.data_cleansing(pd.read_excel(rdata, 'PER'))
psr = ut.data_cleansing(pd.read_excel(rdata, 'PSR'))
pbr = ut.data_cleansing(pd.read_excel(rdata, 'PBR'))

def get_universe(rebalDate, df_info, df_mktcap, df_netincome, df_vol, df_caution_1, df_caution_2, df_caution_3):   
# 코스피 소형주 대상
    lookback_info = df_info.loc[:rebalDate, :].tail(1).transpose()
    lookback_info = lookback_info[lookback_info == 3].dropna().index.values
#    print(len(lookback_info))    
    # 3년 연속 적자기업 제외
    lookback_NI = df_netincome.loc[:rebalDate, :].tail(3)
    lookback_NI = (lookback_NI < 0).astype(int).sum()
    lookback_NI = lookback_NI[lookback_NI < 3].dropna().index.values
#    print(len(lookback_NI))        
    # 시총 800억 미만 기업 제외
    lookback_mktcap = df_mktcap.loc[:rebalDate, :].tail(1).transpose()
    lookback_mktcap = lookback_mktcap[lookback_mktcap > 1000].dropna().index.values # 시총 800억 미만 걸러내기
#    print(len(lookback_mktcap))       
    # 반기 거래대금 3억 미만 기업 제외
    lookback_vol = df_vol.loc[:rebalDate, :].tail(1).transpose()
    lookback_vol = lookback_vol[lookback_vol >= 3].dropna().index.values    
    # 관리종목, 거래정지, 투자유의 종목 제외
    c1 = df_caution_1.loc[:rebalDate, :].tail(1).transpose()
    c1 = c1[c1 == 0].dropna().index.values
    c2 = df_caution_2.loc[:rebalDate, :].tail(1).transpose()
    c2 = c2[c2 == 0].dropna().index.values
    c3 = df_caution_3.loc[:rebalDate, :].tail(1).transpose()
    c3 = c3[c3 == 0].dropna().index.values
    lookback_caution = list(set(c1).intersection(c2).intersection(c3))    
#    print(len(lookback_vol))    
    univ = set(lookback_info).intersection(lookback_NI).intersection(lookback_mktcap).intersection(lookback_vol).intersection(lookback_caution)
    univ = list(univ)            
    return univ

def get_universe_noConst(rebalDate, df_info):  
    # 코스피 소형주 대상
    lookback_info = df_info.loc[:rebalDate, :].tail(1).transpose()
    univ = list(lookback_info[lookback_info == 3].dropna().index.values)
    return univ

def get_priceMom(rebalDate, codes):

    past = ut.get_recentBday(rebalDate - datetime.timedelta(365))
    recent = ut.get_recentBday(rebalDate - datetime.timedelta(30))
    rebalDate = ut.get_recentBday(rebalDate)
    prices = ut.get_stock_price(codes, past, rebalDate )
    mom_12M = (prices.loc[rebalDate, :] / prices.loc[past, :]) - 1
    mom_1M = (prices.loc[rebalDate, :] / prices.loc[recent, :]) - 1
    momData = (mom_12M - mom_1M).dropna()
    return momData   

def to_portfolio(codes, rebalDate, weight = 'equal'):       
    df = pd.DataFrame(index = range(len(codes)), columns = ['date','code', 'weight'])
    df['code'] = codes
    df['weight'] = np.ones(len(codes)) / len(codes)
    df['date'] = rebalDate
    return df

# 1) 0 < PER < 20 & Price mom
#from tqdm import tqdm
import util as ut
import backtest_pipeline as bt
rebal_sche = mkt_info.index[13:-1]

def get_universe(rebalDate, df_info, df_mktcap, df_netincome, df_vol, df_caution_1, df_caution_2, df_caution_3):   
# 코스피 소형주 대상
    lookback_info = df_info.loc[:rebalDate, :].tail(1).transpose()
    lookback_info = lookback_info[lookback_info == 3].dropna().index.values
#    print(len(lookback_info))    
    # 3년 연속 적자기업 제외
    lookback_NI = df_netincome.loc[:rebalDate, :].tail(1)
    lookback_NI = (lookback_NI < 0).astype(int).sum()
    lookback_NI = lookback_NI[lookback_NI < 1].dropna().index.values
#    print(len(lookback_NI))        
    # 시총 800억 미만 기업 제외
    lookback_mktcap = df_mktcap.loc[:rebalDate, :].tail(1).transpose()
    lookback_mktcap = lookback_mktcap[lookback_mktcap > 800].dropna().index.values # 시총 800억 미만 걸러내기
#    print(len(lookback_mktcap))       
    # 반기 거래대금 3억 미만 기업 제외
    lookback_vol = df_vol.loc[:rebalDate, :].tail(1).transpose()
    lookback_vol = lookback_vol[lookback_vol >= j].dropna().index.values    
    # 관리종목, 거래정지, 투자유의 종목 제외
    c1 = df_caution_1.loc[:rebalDate, :].tail(1).transpose()
    c1 = c1[c1 == 0].dropna().index.values
    c2 = df_caution_2.loc[:rebalDate, :].tail(1).transpose()
    c2 = c2[c2 == 0].dropna().index.values
    c3 = df_caution_3.loc[:rebalDate, :].tail(1).transpose()
    c3 = c3[c3 == 0].dropna().index.values
    lookback_caution = list(set(c1).intersection(c2).intersection(c3))    
#    print(len(lookback_vol))    
    
    # 거래대금, 시총500, 2년 연속 적자 반영한 경우
    univ = set(lookback_info).intersection(lookback_mktcap).intersection(lookback_vol)#.intersection(lookback_NI)
    univ = list(univ)            
    return univ

port_list_cons = []
for i in range(len(rebal_sche)):    
    rebal = rebal_sche[i]
    universe = get_universe(rebal, mkt_info, mktcap, net_income, vol, caution_1, caution_2, caution_3)
    if len(universe) < 5:
        pass
    else:
        per_i = per.loc[rebal, universe]
        per_i = per_i[(per_i > 0) & (per_i <=20)].dropna().index.values
        mom_i = get_priceMom(rebal, per_i)
        mom_i = mom_i.nlargest(50)
        codes_i = mom_i.index.values        
        port_i = to_portfolio(codes_i, rebal)
        
        if len(universe) > 10:
            port_list_cons.append(port_i)

        else:
            pass
        
#        print(rebal)

port_data_cons = pd.concat(port_list_cons, ignore_index = True)        
res, tc, to, _ = bt.get_backtest_history(500000000, port_data_cons, roundup=True, tradeCost=0.007)
res = res* (1000 / 500000000)
bm = ut.get_index_price(['I.004'], res.index[0], res.index[-1])/100
(pd.concat([res, bm], axis=1).dropna().pct_change().fillna(0) + 1).cumprod().plot()


import xlwings as xw
wb = xw.Book('results.xlsx')
sht = wb.sheets['Sheet1']

sht.range('A1').value = pd.concat([res, bm], axis=1).dropna()
