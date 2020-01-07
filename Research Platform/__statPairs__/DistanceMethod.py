
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 30 13:10:29 2017

@author: Woojin
"""

import pandas as pd
import numpy as np
import os

# 0. Load Data

workingDirectory = '/Users/Woojin/Desktop/17_Fall/FE537 Statistical Arbitrage/Group Project/Data'
os.chdir(workingDirectory)
rawVolume = pd.read_excel('kospi200.xlsx', 'volume')
rawVolume.index = rawVolume['Symbol Name']
rawVolume = rawVolume.iloc[:,1:]
rawPrice = pd.read_excel('kospi200.xlsx','price')
rawPrice.index = rawPrice['Symbol Name']
rawPrice = rawPrice.iloc[:,1:]

##############################
# 1. Formation Period
##############################
def get_periodicData(data, startYear, startMonth, endYear, endMonth):
    start = str(startYear) + '-' + str(startMonth)
    end = str(endYear) + '-' + str(endMonth)
    return data.loc[start:end][:]


# 1) Set Formation Period
# 해당 기간 동안 거래일 없는거 삭제
fVolume = rawVolume[rawVolume.index.year == 2010]
fVolume = fVolume.dropna(1, 'any')
fVolume = fVolume.loc[:, (fVolume > 0).all(axis=0)]  # 거래량 0 이상인 애들만 Screen out
firmList = list(fVolume.columns)  # 거래량 0 이상인 기업들의 이름 리스트

fPrice = rawPrice[firmList] # 남는 애들만 불러와서  Price를 첫날 가격으로 Normalize
fPrice = fPrice[fPrice.index.year == 2010]  # 연도가 2010년인 애들만 고름

def normalize(data):
    for i in range(len(data.columns)):
        data.iloc[:,i] = data.iloc[:,i] / data.iloc[0,i]
    return data

fPrice = normalize(fPrice)   
 
# 2) Create combination of the firms
import itertools  # 2개 페어 만들기 위한 툴 import
pairs = list(itertools.combinations(firmList, 2)) # 가능한 모든 페어들의 리스트 만들기

# 3) Calculate SSD of each pairs
def get_ssd(price1, price2):
    ssd = sum((price1 - price2) ** 2)
    return ssd

firm_1 = []
firm_2 = []
ssd = []

for i in range(len(pairs)):
    f_1 = pairs[i][0]
    f_2 = pairs[i][1]
    
    p_1 = fPrice[f_1]
    p_2 = fPrice[f_2]
    
    ssd_ = get_ssd(p_1, p_2)
    
    firm_1.append(f_1)
    firm_2.append(f_2)
    ssd.append(ssd_)
    
formationSSD = pd.DataFrame({'firm_1': firm_1, 'firm_2': firm_2, 'SSD': ssd})

# Find top 20 SSD firms combination
pairList = formationSSD.sort_values(by='SSD').head(20)


##############################
# 2. Trading Period
##############################

pair = list(pairList.iloc[0,1:])

"""
1개 회사 (minimum SSD) 로 테스트 해보고 나중에 20개 Firms로 늘려보기 
- Firm 1 : LS
- Firm 2 : LS 산전
"""
pairPrice = rawPrice.loc[:][pair]
pairPrice = normalize(pairPrice)

sd = (pairPrice.loc['2010':'2010'][pair[0]] - pairPrice.loc['2010':'2010'][pair[1]]).std()

tradingPrice = get_periodicData(pairPrice, 2011, 1, 2011, 6)
enterPoint = []
closePoint = []
diffSize = []
diffSize2 = []
for i in range(len(tradingPrice)):
    diff =  abs(tradingPrice.iloc[i,0] - tradingPrice.iloc[i,1]) 
    if diff >= 2*sd:
        enterPoint.append(i)
        diffSize.append(diff)
    elif diff <= sd:
        closePoint.append(i)
        diffSize2.append(diff)
    else:
        pass

(rawPrice.loc['2010-01':'2011-06'][pair[0]]- rawPrice.loc['2010-01':'2011-06'][pair[1]]).plot()