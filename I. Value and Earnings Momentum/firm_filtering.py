# -*- coding: utf-8 -*-
"""
Created on Wed Apr 24 10:16:29 2019

@author: Woojin
"""

path_data = 'C:/Woojin/##. To-do/value_earnMom 전략/rawData'
path_code = 'C:/Woojin/###. Git/Project_Q/I. Value and Earnings Momentum'
import os
import pandas as pd
import numpy as np
import datetime
import calendar
import pymysql
from tqdm import tqdm

os.chdir(path_code)
import util
os.chdir(path_data)

##############################################################################
# 0. 데이터 로드 & 클렌징 
##############################################################################
'''
재무데이터 : 퀀티와이즈 
가격데이터 : 퀀트DB
'''
# 재무정보
ocf = util.data_cleansing(pd.read_excel('fin.xlsx', sheet_name = 'ocf_Q'))
cfTTM = util.data_cleansing(pd.read_excel('fin.xlsx', sheet_name = 'cfTTM_Q'))
ocfTTM = util.data_cleansing(pd.read_excel('fin.xlsx', sheet_name = 'ocfTTM_Q'))
opmTTM = util.data_cleansing(pd.read_excel('fin.xlsx', sheet_name = 'opm_Q'))

# 수급 및 유동성 정보 (20일 거래대금, 20일누적 기관순매수수량, 시가총액, 상장주식수)
vol_20MA = util.data_cleansing_ts(pd.read_excel('liq.xlsx', sheet_name = 'vol_20MA_M'))
netbuy20 = util.data_cleansing_ts(pd.read_excel('liq.xlsx', sheet_name = 'netbuy20_M'))
mktcap = util.data_cleansing_ts(pd.read_excel('liq.xlsx', sheet_name = 'mktcap_M'))
numStock = util.data_cleansing_ts(pd.read_excel('liq.xlsx', sheet_name = 'numStock_M'))

# 상장시장, 섹터, 거래정지 여부 등 기본 정보
market = util.data_cleansing_ts(pd.read_excel('info.xlsx', sheet_name ='market_M'))
sector = util.data_cleansing_ts(pd.read_excel('info.xlsx', sheet_name ='sector_M'))
risk_1 = util.data_cleansing_ts(pd.read_excel('info.xlsx', sheet_name ='risk_1_M'))
risk_2 = util.data_cleansing_ts(pd.read_excel('info.xlsx', sheet_name ='risk_2_M'))
inK200 = util.data_cleansing_ts(pd.read_excel('info.xlsx', sheet_name ='inK200_M'))
inKQ150 = util.data_cleansing_ts(pd.read_excel('info.xlsx', sheet_name ='inKQ150_M'))

# 종목 및 BM 가격 데이터
allFirm = market.columns.values
rPrice_firm = util.get_stock_price(allFirm, pd.to_datetime(market.index.values[0]) - datetime.timedelta(days = 365), market.index.values[-1])
#rPrice_bm = util.get_index_price(['I.001', 'I.101'], market.index.values[0], market.index.values[-1])
return_20D = rPrice_firm.pct_change(20)



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
    rebalDate_ = pd.to_datetime(rebalDate_)
    
    if (rebalDate_.month >=3) & (rebalDate_.month <= 5):
        t_year = rebalDate_.year - 1
        t_month = 12
        
    elif (rebalDate_.month >= 6) & (rebalDate_.month <= 8):
        t_year = rebalDate_.year
        t_month = 3
        
    elif (rebalDate_.month >= 9) & (rebalDate_.month <= 11):
        t_year = rebalDate_.year
        t_month = 6        

    elif rebalDate_.month == 12:
        t_year = rebalDate_.year
        t_month = 9    

    elif (rebalDate_.month >= 1) & (rebalDate_.month <= 2):
        t_year = rebalDate_.year - 1
        t_month = 9    

    else:
        print('데이터 확인 필요')  

    t_day = calendar.monthrange(t_year, t_month)[1]  
    date_available = pd.datetime(t_year, t_month, t_day)
    
    # 최근 분기 영업이익이 플러스인 종목만
    ocfRecent = ocfData.loc[:date_available, :].iloc[-1,:]
    ocfFiltered = plusZero(ocfRecent).index.values 
    # 최근 4분기 현금흐름의 합이 전분기 대비 증가한 종목만
    cfTTMPctRecent = cfTTMData.pct_change().loc[:date_available, :].iloc[-1,:]
    cfTTMFiltered = plusZero(cfTTMPctRecent).index.values       
    # 최근 4분기 영업이익이 전분기 대비 증가한 종목만
    opmTTMPctRecent = opmTTMData.pct_change().loc[:date_available, :].iloc[-1,:]
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
    mktcapToday =mktcapData.loc[:rebalDate_, :].iloc[-1,:]
    mktcapFiltered = mktcapToday[mktcapToday > mktcapLimit].dropna().index.values   
    tradeVolToday = tradeVolData.loc[:rebalDate_, :].iloc[-1,:]
    tradeVolFiltered = tradeVolToday[tradeVolToday > tradeVolLimit].dropna().index.values       
    filtered = list(set(mktcapFiltered).intersection(tradeVolFiltered))   
    return filtered    

def demandFilter(netbuyData, numStockData, rebalDate_, threshold = 0.0):
    '''기관 수급 기준 20일 누적 기관 수급이 순매수인 종목 필터링
    
    거래대금 및 시총과 마찬가지로 당일 데이터 사용하기 떄문에 리밸런싱일자와 같은 날짜인덱스의 데이터 추출
    상대강도 비교를 위해 순매수대금이 아닌 '순매수수량 / 상장주식수' 사용
    '''   
    netbuyToday = netbuyData.loc[:rebalDate_, :].iloc[-1,:]
    numStockToday = numStockData.loc[:rebalDate_, :].iloc[-1,:]    

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
    recentReturn = recentReturn.dropna(axis = 1, how='all')
    filteredNames = recentReturn.columns.values
    upper15_filtered = []
    
    for firm in filteredNames:
        #print(firm)
        # 최근 250 거래일 기준 상위 15% 보다 낮은 수익률일 경우만 넣음
        upper15_ = recentReturn[firm].nlargest(int(250 * retThreshold)).tail(1).values[0]  #상위 15% 수익률
        recent_ = ReturnData_20D.loc[:rebalDate_, firm].tail(1).values[0] #최근 수익률
        
        if upper15_ > recent_:
            upper15_filtered.append(firm) # 상위 15% 수익률 대비 최근 수익률이 낮아야 포함
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

for i in tqdm(range(len(rebalDateList))):

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
# 비중 부여
##############################################################################    
'''
K200 비중을 받아서 당시 비중에 OW / UW 하는 방식으로 비중 부여
K200 비중 받아오되, 거래정지 관리종목 외감인 종목은 제외하고 노말라이즈
'''

#rebalDataFinal = pd.read_excel('C:/Woojin/##. To-do/value_earnMom 전략/rawData/res/firms_190517.xlsx')
#rebalDataFinal_k200 = pd.read_excel('C:/Woojin/##. To-do/value_earnMom 전략/rawData/res/firms_190517_k200.xlsx')

from pandas.tseries.offsets import MonthEnd
from copy import deepcopy
os.chdir(path_data)

k200 = pd.read_excel('kospi200_hist.xlsx')
k200 = k200.iloc[1:,:]
k200 = k200[['Y/M', 'Code', 'Weight(BM)']]
k200['Y/M'] = pd.to_datetime(k200['Y/M'], format="%Y/%m") + MonthEnd(0) # Convert to Month End
k200.columns = ['date', 'code', 'k200_weight']

risk_1.index = risk_1.index + MonthEnd(0)
risk_2.index = risk_2.index + MonthEnd(0)  

for idx in tqdm(k200.index):
    notRisk_1 = risk_1.loc[k200.loc[idx, 'date'], :]
    notRisk_1 = notRisk_1[notRisk_1 == 0].index.values
    notRisk_2 = risk_2.loc[k200.loc[idx, 'date'], :]
    notRisk_2 = notRisk_2[notRisk_2 == 0].index.values
    
    # 거래정지 & 관리종목은 비중에서 삭제
    if (k200.loc[idx, 'code'] in notRisk_1) & (k200.loc[idx, 'code'] in notRisk_2):
        pass
    else:
        #print(idx)
        k200.loc[idx, 'k200_weight'] = 0
        
k200['k200_weight'] = k200['k200_weight'] / k200.groupby('date')['k200_weight'].transform('sum') # 지수 비중 노멀라이즈
k200 = k200[k200['k200_weight'] != 0] # 거래정지 & 관리종목 데이터 삭제
k200 = k200.reset_index().iloc[:,1:]

# KOSPI내 추출종목 중 KOSPI200을 대체하지 않고 그냥 추가
    
def addKOSPIfirms(rebalData, k200WeightData, marketData, sectorData, sectorOn = True, replacement = True, addKQ = True):

    marketData.index = marketData.index + MonthEnd(0)
    sectorData.index = sectorData.index + MonthEnd(0)
        
    y = deepcopy(rebalData)
    
    y['market'] = np.NaN
    y['sector'] = np.NaN
    y['date'] = y['date'] + MonthEnd(0)    
    y['market'] = [marketData.loc[y.loc[idx, 'date'], y.loc[idx,'code']] for idx in y.index]       

    y = pd.merge(k200WeightData, y, how='outer', left_on = ['date', 'code'], right_on = ['date', 'code'], indicator=True)
    
    if addKQ == True:
        y = y
    else:
        y = y[y.market != 'KOSDAQ']   
        
    y = y[(y.date >= '2006-12-31') & (y.date < '2019-05-01') ]  # 이전 데이터는 K200 비중이 없으므로 제외
 

    if replacement == False:
        y['newWeight'] = np.nan        
        for idx in y.index:
            if y.loc[idx, '_merge'] == 'right_only':   # 종목 선정된 KOSPI 애들 중에 K200이 아니었던 애들
                y.loc[idx, 'newWeight'] = 0.01
            elif y.loc[idx, '_merge'] == 'left_only':  # 기존 K200 중 종목선정이 되지 않았던 애들
                y.loc[idx, 'newWeight'] = y.loc[idx, 'k200_weight']
            elif y.loc[idx, '_merge'] == 'both':
                y.loc[idx, 'newWeight'] = y.loc[idx, 'k200_weight'] + 0.01  # K200 중에서 종목선정이 된 애들
        if sectorOn == False: 
            # Normalize (리밸런싱 일자 기준)
            y.newWeight = y.newWeight / y.groupby('date')['newWeight'].transform('sum')
            y = y[['date', 'code', 'newWeight']]
            y.columns = ['date', 'code', 'weight']
        else:
            for idx in y.index:
                y.loc[idx, 'sector'] = sectorData.loc[y.loc[idx, 'date'], y.loc[idx,'code']]
            y['sector_weight'] = y.groupby(['date', 'sector'])['k200_weight'].transform('sum') # 기존 섹터별 비중
            y['newWeight'] = y['newWeight'] / y.groupby(['date', 'sector'])['newWeight'].transform('sum') * y['sector_weight']
            y = y[['date', 'code', 'newWeight']]
            y.columns = ['date', 'code', 'weight']         
       

    else:
        
        y['newWeight'] = np.nan
        dd = {}
        for date, g in y.groupby('date'):
            #print(date)
            # 신규 편입된 비 K200 종목의 숫자 확인
            numOut = len(g[g._merge == 'right_only'])
            # 기존 K200에서 제외되는 종목의 시가총액 하한선 설정
            if numOut == 0:
                mktcap_threshold = 0
            else:
                mktcap_threshold = g[g._merge == 'left_only'].nsmallest(numOut, columns = 'k200_weight')['k200_weight'].max()
            
            g['newWeight'] = np.where(g['_merge'] == 'right_only', 0.01, 
                                      np.where(g['_merge'] == 'both', g['k200_weight'] + 0.01,
                                               np.where((g['_merge'] == 'left_only') & (g['k200_weight'] > mktcap_threshold),
                                                        g['k200_weight'], np.nan)))
            g = g[~pd.isna(g['newWeight'])]
            dd[date] = g
        
        y = pd.concat(dd).reset_index()[['date', 'code', 'k200_weight', 'sector', 'newWeight']]
        
        if sectorOn == False: 
            # Normalize (리밸런싱 일자 기준)
            y.newWeight = y.newWeight / y.groupby('date')['newWeight'].transform('sum')
            y = y[['date', 'code', 'newWeight']]
            y.columns = ['date', 'code', 'weight']
        else:
            for idx in y.index:
                y.loc[idx, 'sector'] = sectorData.loc[y.loc[idx, 'date'], y.loc[idx,'code']]
            y['sector_weight'] = y.groupby(['date', 'sector'])['k200_weight'].transform('sum') # 기존 섹터별 비중
            y['newWeight'] = y['newWeight'] / y.groupby(['date', 'sector'])['newWeight'].transform('sum') * y['sector_weight']            
            y = y[['date', 'code', 'newWeight']]
            y.columns = ['date', 'code', 'weight']   
                
    return y


df1 = addKOSPIfirms(rebalDataFinal, k200, market, sector, replacement=False, sectorOn = True, addKQ = False)     # 기존 바스켓에 더하는 경우
df2 = addKOSPIfirms(rebalDataFinal, k200, market, sector, replacement=True, sectorOn = True, addKQ = False)  # 기존 바스켓에 있는 하위종목을 코스피로 교체
df3 = addKOSPIfirms(rebalDataFinal_k200, k200, market, sector, replacement=False, sectorOn = True, addKQ = False)  #K200 ONly
df4 = addKOSPIfirms(rebalDataFinal, k200, market, sector, replacement=True, sectorOn = True, addKQ = True)  # 기존 바스켓 하위 종목을 코스피, 코스닥으로 교체 

##############################################################################
# 비중 최적화
##############################################################################   
os.chdir(path_code)
import optimization as opt

df5 = opt.optimizedSchedule(df1, 0.005)
df6 = opt.optimizedSchedule(df2, 0.005)
df7 = opt.optimizedSchedule(df3, 0.005)
df8 = opt.optimizedSchedule(df4, 0.005)

newPath = path_data + '/res'
os.chdir(newPath)

rebalDataFinal.to_excel('firms_190524.xlsx', index = False)    
rebalDataFinal_k200.to_excel('firms_190524_k200.xlsx', index = False)  

writer = pd.ExcelWriter('basket_190524.xlsx', engine = 'xlsxwriter')        
df1.to_excel(writer, sheet_name = 'addKOSPI', index = False)
df2.to_excel(writer, sheet_name = 'replacedwithKOSPI', index = False)
df3.to_excel(writer, sheet_name = 'onlyK200', index = False)
df4.to_excel(writer, sheet_name = 'addKOSDAQ', index = False)
df5.to_excel(writer, sheet_name = 'addKOSPI_opt', index = False)
df6.to_excel(writer, sheet_name = 'replacedwithKOSPI_opt', index = False)
df7.to_excel(writer, sheet_name = 'onlyK200_opt', index =False)
df8.to_excel(writer, sheet_name = 'addKOSDAQ_opt', index = False)
writer.save()



