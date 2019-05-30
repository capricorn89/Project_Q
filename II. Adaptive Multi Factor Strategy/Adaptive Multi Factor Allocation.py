# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 17:11:54 2019

@author: Woojin
"""


##############################################################################
# Raw Data Loading........
##############################################################################

import pandas as pd
import numpy as np
import datetime
import os
import matplotlib.pyplot as plt
import sys


def get_platform():
    platforms = {
        'linux1' : 'Linux',
        'linux2' : 'Linux',
        'darwin' : 'OS X',
        'win32' : 'Windows'
    }
    if sys.platform not in platforms:
        return sys.platform
    
    return platforms[sys.platform]

win_path = 'C:/Woojin/###. Git/Project_Q/II. Adaptive Multi Factor Strategy'
mac_path = '/Users/Woojin/Desktop/2019 Project/Adaptive Factor Allocation'

if get_platform() == 'Windows':
    path = win_path
elif get_platform() == 'OS X':
    path = mac_path

os.chdir(path)

import PerformanceEvaluation as perf

'''
I. Factor Return (Proxy로 해당 K200 가중지수의 종가 사용. 단, 고배당지수는 배당률 상위 50종목의 배당가중지수의 개념임)

 1. Size : K200 동일가중지수 (IKS500)
 2. Value : K200 밸류가중지수 (IKS400)
 3. Quality : K200 퀄리티가중지수 (IKS410)
 4. Momentum : K200 모멘텀가중지수 (IKS420)
 5. Low Vol : K200 로우볼가중지수 (IKS430)
 6. High Yield : K200 고배당지수 (IKS225)
 
II. Macro Cycle Indicators (Paper와 유사한 Proxy가 있는 2개 지수 - OECD, ESI 두 개 이용)

 1. OECD 경기선행지수 (OECD CLI)
 2. ESI (경제심리지수, Chicago Fed NAI 대용)
 
 3.  BSI (기업경기실사지수, ISM PMI 지수 대용)  - 들쭉날쭉해서 탈락
 4. ? (Fed Philadelphia ADS Index 대용) - 못구함

III. Valuation Indicators

 1. P/E
 2. P/B
 3. P/CE (CE : Cash Earnings)
  
IV. Market Sentiment Indicators

 1. VKOSPI
 2. Credit Spread ( BAML US Corp BBB OAS 대용) 
 
** Momentum Indicators는 Factor Return 데이터 이용해서 생성하면 되기 때문에 설명 생략 
 
'''

factorPrice = pd.read_excel('macrodata.xlsx', sheet_name = 'factor')  # 지수 수익률 로드

def data_cleansing_ts(rawData):  # 퀀티 데이터 클렌징 함수 (위에 지저분한 열,행 날리기)    
    firmCode = rawData.iloc[6, 1:].values
    dateIndex = rawData.iloc[13:, 0].values
    newData = rawData.iloc[13:,1:]
    newData.columns = firmCode
    newData.index = dateIndex
    return newData

factorPrice = data_cleansing_ts(factorPrice)
factorPrice.columns = ['market', 'size', 'value', 'quality', 'momentum', 'lowvol']

# 데일리 수익률
factorReturn = factorPrice.pct_change()
factorReturn['EW'] = factorReturn[['size', 'value', 'quality', 'momentum','lowvol']].mean(axis=1)
(factorReturn + 1).cumprod().plot(figsize=(10,6)) #데일리 팩터별 상대수익률 

# 매달 10번째 거래일 추출하여 월간 수익률 계산 (지난 달의 macro 지표가 통상 월초에 나오는 것을 감안)
factorPrice_10th = factorPrice.groupby(pd.Grouper(freq='M')).nth(10)
factorReturn_10th = factorPrice_10th.pct_change()
factorReturn_10th['EW'] = factorReturn_10th[['size', 'value', 'quality', 'momentum','lowvol']].mean(axis=1)  # 동일가중은 팩터수익률의 평균
(1+factorReturn_10th).cumprod().plot(figsize=(10,6))  # 월간 팩터별 상대수익률


##############################################################################
# I. Macro Based Factor Allocation
##############################################################################

'''
Macro Based Factor Allocation
: 경기 사이클에 따라 팩터 노출을 다르게 가져가는 전략 
  Size, value는 Cyclical인 반면, Low-vol, Quality는 Defensive한 성질 이용

: 각 지표별로 경기를 4개 국면으로 나누고 해당 국면마다 투자하는 팩터가 달라짐

_______________________________________________________________________________

     국면      |                   기준                    |      투자 팩터
_______________________________________________________________________________
Recovery       |  3MA(t) < 12MA(t), 3MA(t) > 3MA(t-1)      | Value, Size, Yield
_______________________________________________________________________________
Expansion      |  3MA(t) > 12MA(t), 3MA(t) > 3MA(t-1)      | Momentum, Size, Value
_______________________________________________________________________________
Slowdown       |  3MA(t) > 12MA(t), 3MA(t) < 3MA(t-1)      | Momentum, Quality, LowVol
_______________________________________________________________________________
Contraction    |  3MA(t) < 12MA(t), 3MA(t) < 3MA(t-1)      | LowVol, Quality, Value
_______________________________________________________________________________

- 각 Macro Indicator 별로 3개의 팩터에 투자하는 방법 (OECD_CLI, ESI)
- Macro Indicator 여러개를 합쳐서 국면을 결정한 뒤 3개의 팩터에 투자하는 방법 (Integrated Macro)
- 전체 6개 팩터에 동일가중으로 투자하는 경우 (EW)

세 가지에 걸쳐 비교 가능. 따라서 여기서는 OECD 선행지수, ESI, Integrated Macro, 

'''

# 1. 각 지표별로 3MA, 12MA 계산하고 그에 따라 국면 부여 (R, E, S, C)
macroData = pd.read_excel('macroData.xlsx', sheet_name = 'macro').set_index('Date')[['OECD_CLI','ESI']]
numIndicator = len(macroData.columns)
for i in range(numIndicator):
    
    colName_3 = macroData.columns[i] + '_3MA'
    colName_12 = macroData.columns[i] + '_12MA'
    colName_ind = macroData.columns[i] + '_indic'
    
    macroData[colName_3] = macroData[macroData.columns[i]].rolling(3).mean() # 과거 3개월 평균
    macroData[colName_12] = macroData[macroData.columns[i]].rolling(12).mean()  # 과거 12개월 평균
    macroData[colName_ind] = np.nan
    macroData[colName_ind] = (macroData[colName_ind]).astype(str)  # 빈 칼럼에 String을 집어넣으면 에러 발생해서 임시 방편으로..
    
    for j in range(len(macroData)-1):  # 처음 데이터는 비교 대상 없으므로 제외
        
        if (macroData[colName_3][j+1] < macroData[colName_12][j+1]) and (macroData[colName_3][j+1] >= macroData[colName_3][j]):
            macroData[colName_ind][j+1] = str('R')  # Recovery
            
        elif (macroData[colName_3][j+1] >= macroData[colName_12][j+1]) and (macroData[colName_3][j+1] >= macroData[colName_3][j]):
            macroData[colName_ind][j+1] = str('E')  # Expansion
    
        elif (macroData[colName_3][j+1] >= macroData[colName_12][j+1]) and (macroData[colName_3][j+1] < macroData[colName_3][j]):
            macroData[colName_ind][j+1] = str('S')  # Slowdown

        elif (macroData[colName_3][j+1] < macroData[colName_12][j+1]) and (macroData[colName_3][j+1] < macroData[colName_3][j]):
            macroData[colName_ind][j+1] = str('C')  # Contraction
            
        else:
            continue


# 지표를 부여된 국면에 따라 서로 다른 색으로 표시되도록 Plot
#from matplotlib.collections import LineCollection
#from matplotlib.colors import ListedColormap, BoundaryNorm       
macroIndicator = macroData.dropna(axis=0).iloc[1:,:]
colors = {'R':'y', 'E':'g', 'S':'b', 'C':'r'}  # 회복 : 노란색, 확장 : 초록색, 둔화 : 파란색, 침체 : 빨간색
for i in range(numIndicator):
    fig, ax = plt.subplots(figsize=(10,6))
    valName = macroIndicator.columns[i]
    
    ax.scatter(macroIndicator.index.values, macroIndicator[valName].values, marker='o',
               color = list(macroIndicator[valName + '_indic'].apply(lambda x: colors[x]).values))
    plt.show()

# 2. 각 국면에 맞는 팩터 추출 후 수익률 계산
    
'''
ex. 1월 지표를 바탕으로 2월의 팩터 수익률 가져올 것 
    (1월 지표 확인 후, 1월의 10번째 거래일에 투자 --> 2월의 10번째 거래일에 청산하는 컨셉)
'''
import dateutil.relativedelta

def get_return_byRegime(regimeSeries, returnSeries, indicatorName):  # 국면 데이터, 수익률 데이터를 통해 백테스트 결과 도출
    factors_return_list = [0]
    # 수익률과 국면 신호 매칭(ex. 2010-2-28일 수익률 => 2010년 1월의 Indicator 찾기)
    for i in range(1,len(returnSeries)):
        
        date = returnSeries.index[i]  # 수익률이 나오는 시점 
        date_indicator = date - dateutil.relativedelta(months=+1)  # 지표가 나오는 시점 (수익률이 나오는 시점보다 한 시점 앞)
        regime = regimeSeries.loc[:date_indicator].tail(1).get_values()[0]  # 1개 이전 시점의 국면 확인
        #print(date, regime)
        # 국면에 따라 들어갈 팩터 3개 선정
        if regime == 'R':  # 회복국면 : Value, Size, Yield
            factors = ['value', 'size']
        elif regime == 'E': # 확장국면 : Momentum, Size, Value
            factors = ['value', 'size', 'momentum']
        elif regime =='S':  # 둔화국면 : Momentum, Quality, LowVol
            factors = ['momentum', 'quality', 'lowvol']
        elif regime =='C':  # 침체국면 : LowVol, Quality, Value
            factors = ['value', 'quality', 'lowvol']
        else:
            continue
            
        factors_return = returnSeries.loc[date, factors].mean()  # 선정된 3개 팩터 수익률의 평균값 계산
        factors_return_list.append(factors_return)   
    macroRegimeReturn = pd.DataFrame(factors_return_list, index=returnSeries.index, columns=[indicatorName]) 
    return macroRegimeReturn

regime_OECD = macroIndicator['OECD_CLI_indic']
regime_ESI = macroIndicator['ESI_indic']

macroReturn_OECD = get_return_byRegime(regime_OECD, factorReturn_10th.iloc[:,1:], 'OECD_CLI')  # 첫 열은 시장 수익률이므로 제외
macroReturn_ESI = get_return_byRegime(regime_ESI, factorReturn_10th.iloc[:,1:], 'ESI')  

macroReturn = pd.concat([factorReturn_10th, macroReturn_OECD, macroReturn_ESI], axis=1)  # 기존 팩터수익률과의 비교를 위해 병합
macroReturn['IntegratedMacro'] = macroReturn[['OECD_CLI','ESI']].mean(axis=1)  # Integrated Macro는 Indicator별 수익률을 평균해서 사용
macroReturn.iloc[0,:] = 0

import PerformanceEvaluation as perf
macroReturn_ = perf.analysis(macroReturn, 'M')
macroReturn_.annTE('all')
macroReturn_.get_cumReturnPlot(['market', 'yield', 'ESI'])

##############################################################################
# II. Momentum Based Factor Allocation
##############################################################################

'''

 : 팩터 수익률의 모멘텀에 따라 투자
 : 각 팩터들의 최근 1개월, 6개월, 12개월 모멘텀 계산 --> Cross-Sectional로 각 모멘텀별로 Rank 후 Top 3에 투자 (동일가중, 월별 리밸런싱)

- 12개월 모멘텀을 기준으로 투자하는 경우
- 6개월 모멘텀을 기준으로 투자하는 경우
- 1개월 모멘텀을 기준으로 투자하는 경우
- 전체 6개 팩터에 동일가중으로 투자하는 경우
 
'''

def get_return_byMomentum(factorReturnData, window):  # 팩터 수익률 데이터와 모멘텀 확인 구간을 parameter로 받는 함수 생성

    dateIndex = [factorReturnData.index[window]]
    momentumReturn_list = [0]
    pickedFactorList = [np.NaN]
    for i in range(window, len(factorReturnData)-1):
        
        momentumScore_ = factorReturnData[['size', 'yield', 'value', 'quality', 'momentum', 'lowvol']].pct_change(window).iloc[i,:].rank()
        picked_factor = momentumScore_.sort_values()[:3].index.values
        date = factorReturnData.index[i+1]
        momentumReturn = factorReturnData.loc[date, picked_factor].mean()
        
        dateIndex.append(date)
        momentumReturn_list.append(momentumReturn)    
        pickedFactorList.append(picked_factor)
        
    momentumReturn_list = pd.DataFrame(momentumReturn_list, index=dateIndex)
    pickedFactorList = pd.DataFrame(pickedFactorList, index=dateIndex)


    return momentumReturn_list, pickedFactorList

mom12_return, factorMomentum_12 = get_return_byMomentum(factorReturn_10th, 12)
mom6_return, factorMomentum_6 = get_return_byMomentum(factorReturn_10th, 6)
mom1_return, factorMomentum_1 = get_return_byMomentum(factorReturn_10th, 1)
momentumReturn = pd.concat([mom1_return, mom6_return, mom12_return], axis=1).dropna(axis=0)
momentumReturn.columns = ['1M','6M','12M']
momentumAndMacro = pd.concat([momentumReturn, macroReturn], axis=1).dropna(axis=0)
momentumAndMacro.iloc[0,:] = 0
momentumAndMacro_ = perf.analysis(momentumAndMacro, 'M')
momentumAndMacro_.get_cumReturnPlot(['12M', 'quality', 'yield', 'market', 'EW'])

##############################################################################
#. 시장 수익률의 Undefined Regimes 확인하기?
##############################################################################






##############################################################################
#. Market Sentiment Based Factor Allocation
##############################################################################
'''
Market Sentiment Based Factor Allocation
 : 시장 전반의 Risk-on/off 에 따라 투자
 - VKOSPI (Future) Curve 이용
   : 상향 (Normal. Spot < Future)   ==> Value, Momentum, Size
     하향 (Stressed. Spot > Future) ==> LowVol, Quality, Yield

VKOSPI Future Curve가 제대로 확보되지 않았음 ==> 실패
'''

##############################################################################
# IV. Valuation Based Factor Allocation
##############################################################################
'''
Valuation Based Factor Allocation
 : 팩터들의 상대적인 Valuation 에 따라 투자
 : Time-Series Valuation 이용 (구조적으로 밸류에이션이 싼 팩터가 계속 뽑히는 걸 막기 위해 상대적인 밸류에이션 강도 측정)
 "The valuation spread is computed as the valuation of the factor index 
  relative to the valuation of a six-factor equally weighted mix" 라는데 뭔말이지..

(i) 뭔말인지 모르겠어서
(ii) 팩터별로 밸류에이션 구하는 문제 
때문에 취소
'''

##############################################################################
# V. Adaptive Mix
##############################################################################



##############################################################################
# VI. Top-down vs. Bottom-up
##############################################################################

