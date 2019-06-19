# -*- coding: utf-8 -*-
"""
Created on Tue May 28 15:19:57 2019

@author: Woojin

< Multi-factor Model >

"""

import pandas as pd
import os
import numpy as np
import datetime
import pymysql
from tqdm import tqdm
import calendar
from scipy import stats

os.chdir('C:/Woojin/###. Git/Project_Q/II. Adaptive Multi Factor Strategy')

import backtest_pipeline_ver2 as bt
import util



'''
II. Macro Cycle Indicators (Paper와 유사한 Proxy가 있는 2개 지수 - OECD, ESI 두 개 이용)

 1. OECD 경기선행지수 (OECD CLI)
 2. ESI (경제심리지수, Chicago Fed NAI 대용)
 
 3.  BSI (기업경기실사지수, ISM PMI 지수 대용)  - 들쭉날쭉해서 탈락
 4. ? (Fed Philadelphia ADS Index 대용) - 못구함
 
 Macro Based Factor Allocation
: 경기 사이클에 따라 팩터 노출을 다르게 가져가는 전략 
  Size, value는 Cyclical인 반면, Low-vol, Quality는 Defensive한 성질 이용

: 각 지표별로 경기를 4개 국면으로 나누고 해당 국면마다 투자하는 팩터가 달라짐

_______________________________________________________________________________________

     국면      |                   기준                    |      투자 팩터
_______________________________________________________________________________________
Recovery       |  3MA(t) < 12MA(t), 3MA(t) > 3MA(t-1)      | Value, Size, Yield
_______________________________________________________________________________________
Expansion      |  3MA(t) > 12MA(t), 3MA(t) > 3MA(t-1)      | Momentum, Size, Value
_______________________________________________________________________________________
Slowdown       |  3MA(t) > 12MA(t), 3MA(t) < 3MA(t-1)      | Momentum, Quality, LowVol
_______________________________________________________________________________________
Contraction    |  3MA(t) < 12MA(t), 3MA(t) < 3MA(t-1)      | LowVol, Quality, Value
_______________________________________________________________________________________

- 각 Macro Indicator 별로 3개의 팩터에 투자하는 방법 (OECD_CLI, ESI)
- Macro Indicator 여러개를 합쳐서 국면을 결정한 뒤 3개의 팩터에 투자하는 방법 (Integrated Macro)
- 전체 6개 팩터에 동일가중으로 투자하는 경우 (EW)

세 가지에 걸쳐 비교 가능. 따라서 여기서는 OECD 선행지수, ESI, Integrated Macro, 
  
'''

# I. Cross-sectional

''' 
ex. Value & Momentum (Sales to Price, Book to Price, Momentum)

'''

def find_regime(macro, rebalDate):    
    lookback = macro.loc[:rebalDate]    
    # 현재 시점 (t)
    avg12_now = lookback.rolling(12).mean()[-1]  # 12개월 평균
    avg3_now = lookback.rolling(3).mean()[-1]  # 3개월 평균

    # 과거 시점 (t-1)
    #avg12_last = lookback.rolling(12).mean()[-2]  # 12개월 평균
    avg3_last = lookback.rolling(3).mean()[-2]  # 3개월 평균

    if (avg3_now < avg12_now) and (avg3_now >= avg3_last):
        regime = 'recovery'
    
    elif (avg3_now >= avg12_now) and (avg3_now >= avg3_last):
        regime = 'expansion'
    
    elif (avg3_now >= avg12_now) and (avg3_now < avg3_last):    
        regime = 'slowdown'
        
    elif (avg3_now < avg12_now) and (avg3_now < avg3_last):
        regime = 'contraction'
        
    else:
        pass
    
    return regime


def get_priceRatio_multi(factorData, mktcapData):
    code_f = factorData.index.values
    code_p = mktcapData.columns.values
    code = list(set(code_f).intersection(code_p))
    #print(code)
    ratioData = pd.DataFrame(index = code, columns = factorData.columns)
    ratioData[factorData.columns] = factorData.loc[code,:].values    
    ratioData['mktcap'] = mktcapData[code].values[0]    
    
    for factors in factorData.columns:
        ratioData[factors] = ratioData[factors] / ratioData['mktcap'] 
    
    return ratioData[factorData.columns].dropna(how='all')


def find_factor(regime):
    
    if regime == 'recovery':
        factor = ['value', 'size', 'yield']       
    elif regime == 'expansion':
        factor = ['momentum', 'size', 'value']
    elif regime == 'slowdown':
        factor = ['momentum', 'quality', 'lowvol']
    elif regime == 'contraction':
        factor = ['lowvol', 'quality', 'value']
    else:
        pass
    return factor


def find_factor_exSize(regime):
    
    if regime == 'recovery':
        factor = ['value', 'yield']       
    elif regime == 'expansion':
        factor = ['momentum', 'value']
    elif regime == 'slowdown':
        factor = ['momentum', 'quality', 'lowvol']
    elif regime == 'contraction':
        factor = ['lowvol', 'quality', 'value']
    else:
        pass
    return factor

start_year, start_month = 2005, 12
start_day = calendar.monthrange(start_year, start_month)[1]
end_year, end_month = 2019, 4
end_day = calendar.monthrange(end_year, end_month)[1]

start_invest = pd.datetime(start_year, start_month, start_day)
end_invest = pd.datetime(end_year, end_month, end_day)
rebal_sche = pd.date_range(start_invest, end_invest, freq = 'M')

# Load market info data
marketInfo = util.data_cleansing_ts(pd.read_excel('factorData.xlsx', sheet_name = 'market'))
mktcap = util.data_cleansing_ts(pd.read_excel('factorData.xlsx', sheet_name = 'mktcap'))
risk_1 = util.data_cleansing_ts(pd.read_excel('factorData.xlsx', sheet_name = 'risk_1'))
risk_2 = util.data_cleansing_ts(pd.read_excel('factorData.xlsx', sheet_name = 'risk_2'))

# Load factor data
factor_book = util.data_cleansing(pd.read_excel('factorData.xlsx', sheet_name = 'book'))
factor_size = util.data_cleansing(pd.read_excel('factorData.xlsx', sheet_name = 'size'))
factor_yield = util.data_cleansing(pd.read_excel('factorData.xlsx', sheet_name = 'dividend'))

current_asset = util.data_cleansing(pd.read_excel('factorData.xlsx', sheet_name = 'CA'))
current_liability = util.data_cleansing(pd.read_excel('factorData.xlsx', sheet_name = 'CL'))
total_liability = util.data_cleansing(pd.read_excel('factorData.xlsx', sheet_name = 'TL'))
accural_ratio = (current_asset- current_liability).pct_change()

earnings = util.data_cleansing(pd.read_excel('factorData.xlsx', sheet_name = 'earnings'))
factor_roe = earnings / factor_book

factor_lever = total_liability / factor_book

# Load Macro data
macroIndicator = pd.read_excel('macroData.xlsx', sheet_name = 'macro').set_index('Date')

regimeList = []
rebalData_long = []
rebalData_short = []
factorNameList = []
num_group = 5
method = 'integrated'
macroData = macroIndicator['ESI']

for i in tqdm(range(len(rebal_sche))):
    
    date_spot = util.get_recentBday(rebal_sche[i], dateFormat = 'datetime')
    univ_ = util.getUniverse(marketInfo, mktcap, risk_1, risk_2, date_spot)
    macro_regime = find_regime(macroData, date_spot)  # 해당시점의 국면 확인
    
    #factor_names = find_factor(macro_regime)  # 확인된 국면에 따라 작동하는 팩터 추출
    factor_names = find_factor_exSize(macro_regime)  # 확인된 국면에 따라 작동하는 팩터 추출 (사이즈 팩터 제외 시)
    
    
    factorNameList.append(factor_names)
    regimeList.append(macro_regime)
    
    # 1. Value
    value_spot = util.getFinancialData(factor_book, date_spot)[univ_] # Start from value factor
    value_spot = pd.concat([value_spot], axis = 1, sort = False)
    value_spot.columns = ['value']
    mktcaps = util.get_mktcap(value_spot.index.values, date_spot, date_spot)  # 시총으로 나눠줘야 함 (시총 데이터 추출)
    value_spot = get_priceRatio_multi(value_spot, mktcaps)  # 북 밸류를 시총으로 나눠줌
    value_spot = util.to_zscore(util.winsorize_df(value_spot))
    
    # 2. Size
    size_spot = -1 * util.getFinancialData(factor_size, date_spot)[univ_] # Add size factor (작을수록 좋아야하기 때문에 -1 곱함. 나중에 z-scor가 큰애가 뽑히기때문)
    size_spot = pd.concat([size_spot], axis = 1, sort = False)
    size_spot = util.to_zscore(util.winsorize_df(size_spot))
    
    # 3. Momentum
    mom_spot = util.get_adjMom(univ_, date_spot)  # Add momentum factor
    mom_spot = pd.concat([mom_spot], axis = 1, sort = False)
    mom_spot = util.to_zscore(util.winsorize_df(mom_spot))
    
    # 4. Yield
    yield_spot = util.getFinancialData_TTM(factor_yield, date_spot)[univ_]  # Add yield factor (직전 4분기 합으로 추출)
    yield_spot = pd.concat([yield_spot], axis = 1, sort = False)
    yield_spot.columns = ['yield']
    yield_spot = get_priceRatio_multi(yield_spot, mktcaps)  # 전체 배당액을 시총으로 나눠 배당수익률 확인
    yield_spot = util.to_zscore(util.winsorize_df(yield_spot))
    
    # 5. LowVol
    inverseVol_spot = util.get_inverseVol(univ_, date_spot)
    inverseVol_spot = pd.concat([inverseVol_spot], axis = 1, sort = False)
    inverseVol_spot = util.to_zscore(util.winsorize_df(inverseVol_spot))
    
    # 6. Quality     
    roe_spot = util.getFinancialData(factor_roe, date_spot)[univ_]
    ar_spot = util.getFinancialData(accural_ratio, date_spot)[univ_] * -1
    lever_spot = util.getFinancialData(factor_lever, date_spot)[univ_] * -1    
    quality_spot = pd.concat([roe_spot, ar_spot, lever_spot], axis = 1, sort= False)
    quality_spot.columns = ['roe', 'ar', 'lever']
    quality_spot = util.to_zscore(util.winsorize_df(quality_spot))
    quality_spot = util.get_multifactor_score(quality_spot)

    # I-1. Integrated Method (Signal Blend, 개별 팩터 스코어를 모두 더해 한번에 주식을 뽑는 방법)
    if method == 'integrated': # 하나의 데이터로 묶음
        port = pd.concat([value_spot, size_spot, mom_spot, yield_spot, inverseVol_spot, quality_spot ], axis= 1, sort=False)
        port.columns = ['value', 'size', 'momentum', 'yield', 'lowvol', 'quality']        
        # 국면에 해당하는 팩터만 선택
        port_regime = port[factor_names]
        port_score = util.get_multifactor_score(port_regime)
        port = pd.qcut(port_score, num_group, labels =False)
        port_long  = util.to_portfolio(port[port  == num_group - 1].index.values, date_spot)  # Long : Highest 20% having total z-score
        port_short = util.to_portfolio(port[port == 0].index.values, date_spot)

        rebalData_long.append(port_long)
        rebalData_short.append(port_short)

    # I-2. Composite Method (개별 팩터별로 익스포저가 높은 종목을 뽑은 뒤 합치는 방법)        
    elif method == 'composite':
        pass
    # I-3. Priority Method (1번 팩터로 거른 뒤 , 2번 팩터로 거르는 식으로 순차적으로 종목을 필터링하는 방법)    
    elif method == 'priority':
        pass
     
    else:
        pass

rebalData_long = pd.concat(rebalData_long).reset_index()[['date','code','weight']]
rebalData_short = pd.concat(rebalData_short).reset_index()[['date','code','weight']]
print('Portfolio completed')

#rebalData_long.to_excel('multifator_basket_long_' + method + '.xlsx')
#rebalData_long.to_excel('multifator_basket_short_' + method + '.xlsx')

result_long = bt.get_backtest_history(1000, rebalData_long)[0]
result_short = bt.get_backtest_history(1000, rebalData_short)[0]
result = pd.concat([result_long, result_short], axis = 1)
result.columns = ['long', 'short']
result['longShort_return'] = np.subtract(result.pct_change()['long'],result.pct_change()['short'])


bm = util.get_index_price(['I.001','I.101'], result_long.index[0], result_long.index[-1])/100
result = pd.concat([result, bm], axis = 1)
result['I.101_return'] = result['I.101'].pct_change()

factorNameList = pd.DataFrame(factorNameList, index = rebal_sche)
regimeList = pd.DataFrame(regimeList, index = rebal_sche)
factorName = pd.concat([regimeList, factorNameList], axis = 1)
date = datetime.datetime.today()
result.to_excel('res_' + str(date.year) + str(date.month) + str(date.day) + '.xlsx')
rebalData_long.to_excel('basket_long_'+ str(date.year) + str(date.month) + str(date.day) + '.xlsx')
rebalData_short.to_excel('basket_short' + str(date.year) + str(date.month) + str(date.day) + '.xlsx')
factorName.to_excel('factorName' + str(date.year) + str(date.month) + str(date.day) + '.xlsx')


##############################################################################
# Plot
##############################################################################

from bokeh.plotting import figure, output_file, show
from bokeh.layouts import column
from bokeh.models import LinearAxis, Range1d
from bokeh.transform import factor_cmap
from bokeh.palettes import Spectral10
from bokeh.models import ColumnDataSource
from bokeh.models import HoverTool

result = pd.read_excel('res_2019618.xlsx')


x = result.index
y1 = (result['longShort_return'].fillna(0)+1).cumprod() * 100
y2 = (result['I.101_return'].fillna(0) + 1).cumprod() * 100
y4 = (result['long'].pct_change().fillna(0) + 1).cumprod() * 100
y3 = y1 - y2
y5 = y4 - y2

p1 = figure(plot_width=800, plot_height=300, 
           x_axis_type="datetime", x_axis_label='Date', y_axis_label='Relative Return')
p1.title.text = "Adaptive Multi Factor Allocatioin using ESI Index (excluding Size factor)"
p1.title.align = "center"
p1.extra_y_ranges = {"foo": Range1d(start=y3.min(), end = y3.max())}
p1.add_layout(LinearAxis(y_range_name = "foo", axis_label = 'Excess Return'), 'right')

p1.line(x, y1, legend="Portfolio", color = 'red', line_width=2)
p1.line(x, y2, legend="KOSPI200", color = 'grey', line_width=2)
p1.line(x, y4, legend="LongOnly", color = 'purple', line_width=2)
p1.varea(x, y1=np.zeros(len(y3)), y2=y3, legend="Excess Return", color = 'green', alpha =0.2, y_range_name="foo")
p1.varea(x, y1=np.zeros(len(y5)), y2=y5, legend="Excess Return (LongOnly)", color='yellow', alpha = 0.4, y_range_name="foo")
p1.legend.location = "top_left"
p1.legend.click_policy="hide"

p2 = figure(plot_width=800, plot_height=100, x_range = p1.x_range, x_axis_type="datetime")
p2.varea(x=x, y1 = np.zeros(len(y1)), y2 = util.get_drawdown(y1), legend="Drawdown")

p2.legend.location = "bottom_left"

show(column(p1,p2))

#hover = p1.select(dict(type=HoverTool))
#hover.tooltips= [("Date", "$x"),
#                ("Portfolio : ", "$y1"),
#                ("KOSPI200 : ", "$y2"), 
#                ("Excess Return : ", "$y3"),
#                ("Portfolio (Long Only) : ", "$y4"),
#                ("Excess Return (Long Only) : ", "$y5")]
#hover.mode = 'mouse' 
##    formatters = {
##                 "Date" : 'datetime',
##                 'y1' : 'printf',
##                 'y2' : 'printf',
##                 'y3' : 'printf',
##                 'y4' : 'printf',
##                 'y5' : 'printf',
##                 },
#show(p1)

