# -*- coding: utf-8 -*-
"""
Created on Wed Apr 24 10:16:29 2019

@author: Woojin Ji
"""

path = 'C:/Woojin/##. To-do/value_earnMom 전략/rawData'

import os
import pandas as pd
import numpy as np
import datetime
import calendar
import pymysql

os.chdir(path)

##############################################################################
# 0. 데이터 로드 & 클렌징
##############################################################################

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


# 종목 및 BM 가격 데이터
rPrice_firm = data_cleansing_ts(pd.read_excel('price.xlsx', sheet_name = 'price_D'))
rPrice_bm = data_cleansing_ts(pd.read_excel('price.xlsx', sheet_name = 'price_BM_D'))
return_20D = rPrice_firm.pct_change(20)

# 재무정보
ocf = data_cleansing(pd.read_excel('fin.xlsx', sheet_name = 'ocf_Q'))
cfTTM = data_cleansing(pd.read_excel('fin.xlsx', sheet_name = 'cfTTM_Q'))
ocfTTM = data_cleansing(pd.read_excel('fin.xlsx', sheet_name = 'ocfTTM_Q'))
opmTTM = data_cleansing(pd.read_excel('fin.xlsx', sheet_name = 'opm_Q'))


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


##############################################################################
# 1. 투자 유니버스 구성
##############################################################################

def getUniverse(marketInfo, volInfo, riskInfo_1, riskInfo_2, rebalDate_, tradeVolLimit = 10):
    '''market에서 외감, 거래정지, 관리종목인 종목 거르고, 20일 평균거래대금 10억 이상으로 필터링'''
    m = marketInfo.loc[rebalDate_, :]
    inMarket = m[m != '외감'].index.values
    
    notRisk_1 = riskInfo_1.loc[rebalDate_, :]
    notRisk_1 = notRisk_1[notRisk_1 == 0].index.values
    
    notRisk_2 = riskInfo_2.loc[rebalDate_, :]
    notRisk_2 = notRisk_2[notRisk_2 == 0].index.values
    
    v = volInfo.loc[rebalDate_, :]
    above10 = v[v >= tradeVolLimit].index.values
    
    res = set((set(inMarket).intersection(above10)).intersection(notRisk_1)).intersection(notRisk_2)
    
    return res

##############################################################################
# 2. 필터링
##############################################################################
def getRecentData(data, rebalDate_):
    '''최근 3기 데이터 로드'''
    return data.loc[:rebalDate_,:].tail(3).dropna(axis=1)

def plusZero(data):
    return data[data > 0].dropna()
        
def gwthFilter(ocfData, cfTTMData, opmTTMData, rebalDate_):
    ''' 성장성기준 필터링 함수
    1. 최근 분기 OCF > 0
    2. 최근 4개분기 CF 합의 증가율 > 0
    3. 최근 4개분기 OPM의 증가율 > 0
    
    가용 최신 데이터 : 매월말 기준으로 해당분기의 직전 분기말 
    
        종목 선정 시점     |   가용데이터 인덱스  (T)   |   가용데이터 인덱스 (T-1)
    -------------------------------------------------------------
            3월말          |        12말              |       9말
            4월말          |        12말              |       9말
            5월말          |        12말              |       9말
            6월말          |         3말              |       12말
            7월말          |         3말              |       12말
            8월말          |         3말              |       12말
            9월말          |         6말              |       3말
           10월말          |         6말              |       3말
           11월말          |         6말              |       3말     
           12월말          |         9말              |       6말    
            1월말          |         9말              |       6말
            2월말          |         9말              |       6말
    '''
    # 최근 분기 영업이익이 플러스인 종목만
    ocfRecent = getRecentData(ocfData, rebalDate_).iloc[1,:]
    ocfFiltered = plusZero(ocfRecent).index.values 
    # 최근 4분기 현금흐름의 합이 전분기 대비 증가한 종목만
    cfTTMPctRecent = getRecentData(cfTTMData.pct_change(), rebalDate_).iloc[1,:]
    cfTTMFiltered = plusZero(cfTTMPctRecent).index.values       
    # 최근 4분기 영업이익이 전분기 대비 증가한 종목만
    opmTTMPctRecent = getRecentData(opmTTMData.pct_change(), rebalDate_).iloc[1,:]
    opmTTMFiltered = plusZero(opmTTMPctRecent).index.values       
    filtered = list(set(set(ocfFiltered).intersection(cfTTMFiltered)).intersection(opmTTMFiltered))
    
    return filtered


def liqFilter(mktcapData, tradeVolData, rebalDate_, mktcapLimit = 2000, tradeVolLimit = 10):
    ''' 거래대금 및 시총 기준 필터링 함수
    1. 시총 2천억 이상 
    2. 20일 일평균거래대금 10억 이상
    
    주의 : 시총 및 거래대금은 리밸런싱 당일의 데이터를 사용하기 때문에 인덱스가 같음
    
    ** 참고/고려사항 : 시점에 따라 시총과 거래대금의 기준이 바뀌어야 하는거 아닌지
    
    '''
    mktcapToday = getRecentData(mktcapData, rebalDate_).iloc[2,:]
    mktcapFiltered = mktcapToday[mktcapToday > mktcapLimit].dropna().index.values   
    tradeVolToday = getRecentData(tradeVolData, rebalDate_).iloc[2,:]
    tradeVolFiltered = tradeVolToday[tradeVolToday > tradeVolLimit].dropna().index.values       
    filtered = list(set(mktcapFiltered).intersection(tradeVolFiltered))   
    return filtered    

def demandFilter(netbuyData, numStockData, rebalDate_, threshold = 0.0):
    '''기관 수급 기준 20일 누적 기관 수급이 순매수인 종목 필터링
    
    거래대금 및 시총과 마찬가지로 당일 데이터 사용하기 떄문에 리밸런싱일자와 같은 날짜인덱스의 데이터 추출
    상대강도 비교를 위해 순매수대금이 아닌 '순매수수량 / 상장주식수' 사용
    '''   
    netbuyToday = getRecentData(netbuy20, rebalDate_).iloc[2,:]
    numStockToday = getRecentData(numStock, rebalDate_).iloc[2,:]    
    buyStrength = netbuyToday / numStockToday   
    demandFiltered = list(buyStrength[buyStrength > threshold].dropna().index.values)   
    return demandFiltered
 

def momentumFilter(filteredNames, ReturnData_20D, marketInfoData, inK200Data, rebalDate_, retThreshold = 0.15):
        
    '''
    최근 N일 수익률 상위 P% 이내에 있는 종목 제외  
    유동성, 수급 등 조건을 통해 필터링된 종목들을 인풋으로 받아야 함
    output : 시장구분없는 전체 종목, k200 종목, kospi(+k200) 종목, kosdaq종목
    '''
    #리밸런싱 기준 20일 수익률 최근 250개 불러오기
    recentReturn = ReturnData_20D.loc[:rebalDate_, filteredNames].tail(250)
    upper15_filtered = []
    for firm in filteredNames:
        # 상위 15% 보다 높은 수익률이면 제외
        upper15_ = recentReturn[firm].nlargest(int(250 * retThreshold)).tail(1).values[0]
        recent_ = ReturnData_20D.loc[:rebalDate_, firm].tail(1).values[0]
        if upper15_ <= recent_:
            upper15_filtered.append(firm)
        else:
            pass
    #시장별로 그룹    
    marketinfo = pd.DataFrame(marketInfoData.loc[rebalDate_, upper15_filtered])
    marketinfo.columns = ['market_info']
    isK200 = inK200Data.loc[:rebalDate_,:].tail().iloc[-1,:]
    isK200 = isK200[isK200 == 1].index.values
    
    for firmName in upper15_filtered:  # KOSPI 중 K200 인 종목은 거래 시장을 K200으로 바꿔줌
        if firmName in isK200:
            marketinfo.loc[firmName, 'market_info'] = 'KOSPI200'
        else:
            pass           
    allNames = marketinfo.index.values
    k200Only = marketinfo[marketinfo['market_info']=='KOSPI200'].index.values
    kospi = marketinfo[(marketinfo['market_info']=='KOSPI') | (marketinfo['market_info']=='KOSPI200')].index.values
    kosdaq = marketinfo[marketinfo['market_info']=='KOSDAQ'].index.values
    
    return allNames, k200Only, kospi, kosdaq



##############################################################################
# BackTest
##############################################################################    

def getDT(numpyDateFormat):
    return pd.to_datetime(np.datetime64(numpyDateFormat))

'''rebalancing schedule 첫날로 테스트 2004년 4월말 (3월말 기준 재무데이터 활용) '''
rebalDateList = mktcap.index.values[3:]



rebalDict = {}
rebalDict_k200 = {}
rebalDict_k200Weight = {}

for i in range(len(rebalDateList)):

    rebalDate = rebalDateList[i]
    univ = getUniverse(market, vol_20MA, risk_1, risk_2, rebalDate)
    gwthFiltered = set(univ).intersection(gwthFilter(ocf, cfTTM, opmTTM, rebalDate))
    liqFiltered = set(gwthFiltered).intersection(liqFilter(mktcap, vol_20MA, rebalDate))
    demFiltered = list(set(liqFiltered).intersection(demandFilter(netbuy20, numStock, rebalDate)))
    
    
    momentumFiltered = momentumFilter(demFiltered, return_20D, market, inK200, rebalDate)
    
    filterFinal = momentumFiltered[0]
    if len(momentumFiltered) >= 1:
        filterFinal_k200 = momentumFiltered[1]
    else:
        filterFinal_k200 = np.nan
    
    rebalDict[rebalDate] = list(filterFinal)
    rebalDict_k200[rebalDate] = list(filterFinal_k200)
    rebalDict_k200Weight[rebalDate] = np.ones(len(filterFinal_k200)) / len(filterFinal_k200)
    
    print("Rebalancing Schedule : ", getDT(rebalDate).year , "/", 
          getDT(rebalDate).month, " And the total num of firms is : ", len(filterFinal), "\n", " K200: ", len(filterFinal_k200))
    #print(list(liqFiltered))
   
    
rebalData = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in rebalDict.items() ])).transpose()
rebalData_k200 = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in rebalDict_k200.items() ])).transpose()

rebalDataFinal = rebalData.unstack(level=0).reset_index().sort_values('level_1')[['level_1',0]].reset_index().iloc[:,1:].dropna()
rebalDataFinal_k200 = rebalData_k200.unstack(level=0).reset_index().sort_values('level_1')[['level_1',0]].reset_index().iloc[:,1:].dropna()

rebalDataFinal.columns = ['date', 'code']
rebalDataFinal_k200.columns = ['date', 'code']

##############################################################################
# BackTest II
##############################################################################    
'''
K200 비중을 받아서 당시 비중에 OW / UW 하는 방식으로 비중 부여
K200 비중 받아오되, 거래정지 관리종목 외감인 종목은 제외하고 노말라이즈
'''

rebalDataFinal = pd.read_excel('C:/Woojin/##. To-do/value_earnMom 전략/rawData/res/firms_190515.xlsx')
rebalDataFinal_k200 = pd.read_excel('C:/Woojin/##. To-do/value_earnMom 전략/rawData/res/firms_190515_k200.xlsx')

from pandas.tseries.offsets import MonthEnd
k200Weight = pd.read_excel('kospi200_hist.xlsx')
k200Weight = k200Weight.iloc[1:,:]
k200Weight = k200Weight[['Y/M', 'Code', 'Weight(BM)']]
k200Weight['Y/M'] = pd.to_datetime(k200Weight['Y/M'], format="%Y/%m") + MonthEnd(0) # Convert to Month End
k200Weight.columns = ['date', 'code', 'k200_weight']

risk_1.index = risk_1.index + MonthEnd(0)
   
for idx in k200Weight.index:
 
    notRisk_1 = risk_1.loc[k200Weight.loc[idx, 'date'], :]
    notRisk_1 = notRisk_1[notRisk_1 == 0].index.values
    
    if k200Weight.loc[idx, 'code'] in notRisk_1:
        pass
    else:
        k200Weight.loc[idx, 'k200_weight'] = 0
        
k200Weight['k200_weight'] = k200Weight['k200_weight'] / k200Weight.groupby('date')['k200_weight'].transform('sum')


def mergeK200Weight(rebalData, k200WeightData, sectorData, sectorOn = True):
    
    def get_sector(date, code, sectorData):
        return sectorData.loc[:date, code].tail(1).values[0]
    
    rebalData['weight'] = 1 # 추출된 종목들에는 비중에 1 부여 --> 나중에 k200 비중보다 더 높은 비중을 줘야하기 떄문
    rebalData['date'] = rebalData['date'] + MonthEnd(0)# 월 마지막날로 날짜 통일
    # K200 비중 데이터와 병합
    rebalData = pd.merge(k200Weight, rebalData, how='outer', left_on = ['date', 'code'], right_on = ['date', 'code'])
    rebalData['newWeight'] = np.nan

    for idx in rebalData.index:
        if pd.isna(rebalData.loc[idx, 'weight']):
            rebalData.loc[idx, 'newWeight'] = rebalData.loc[idx, 'k200_weight']
        else:
            rebalData.loc[idx, 'newWeight'] = rebalData.loc[idx, 'k200_weight'] + 0.01
   
    if sectorOn == False:
        # Normalize (리밸런싱 일자 기준)
        rebalData.newWeight = rebalData.newWeight / rebalData.groupby('date')['newWeight'].transform('sum')
        rebalData = rebalData[['date', 'code', 'newWeight']].dropna() # k200 비중이 있던 시기만 남김 (비교를 위해)
        rebalData.columns = ['date', 'code', 'weight']
        
    else:
        rebalData['sector'] = np.NaN
        for i in range(len(rebalData)):
            rebalData.loc[i, 'sector'] = get_sector(rebalData.loc[i,'date'], rebalData.loc[i, 'code'], sectorData)
        
        rebalData['sector_weight'] = rebalData.groupby(['date', 'sector'])['k200_weight'].transform('sum') # 기존 섹터별 비중
        rebalData['newWeight'] = rebalData['newWeight'] / rebalData.groupby(['date', 'sector'])['newWeight'].transform('sum') * rebalData['sector_weight']
        rebalData = rebalData[['date', 'code', 'newWeight']].dropna() # k200 비중이 있던 시기만 남김 (비교를 위해)
        rebalData.columns = ['date', 'code', 'weight']        
                
    return rebalData



basket_190515 = mergeK200Weight(rebalDataFinal_k200, k200Weight, sector, sectorOn=True)

os.chdir('C:/Woojin/##. To-do/value_earnMom 전략/rawData/res')
rebalDataFinal.to_excel('firms_190515.xlsx', index = False)    
rebalDataFinal_k200.to_excel('firms_190515_k200.xlsx', index = False)    
basket_190515.to_excel('basket_190515.xlsx', index = False)
    



basket_190515[basket_190515.date == '2018-02-28'].sum()








