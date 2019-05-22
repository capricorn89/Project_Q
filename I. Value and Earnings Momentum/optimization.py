# -*- coding: utf-8 -*-
"""
Created on Fri May 17 13:21:49 2019

@author: user
"""

import os
import pandas as pd
import numpy as np
import datetime
import calendar
import pymysql
from scipy.optimize import minimize


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


def maxSharpe(priceData, weight_mkt, threshold):
    
    '''
    변동성 대비 기대수익률을 최대화하는 함수
    
    # Input variables:
     - mu : 개별 종목의 기대수익률 (index : 종목코드, item : 수익률)
     - sigma : 포트의 분산-공분산행렬
     - weight_mkt : 개별 종목의 시장비중
     - threshold : 시장비중 대비 얼마나 오버/언더 가능한지
    
    # Output variables:
     - .x : 개별종목의 최적화된 비중
    
    '''
    returnData = priceData.pct_change()
    mu = np.nanmean(returnData, axis = 0)
    sigma = returnData.cov().values      
            
    # Create function that maximize Sharpe Ratio
    def getSharpe(weight):        
        return_ = weight.dot(mu)
        variance = weight.dot(sigma).dot(weight)
        std = np.sqrt(variance)
        SR = return_/std
        return -SR # max
    
    numAsset = len(mu)
    
    cons_1 = ({'type' : 'eq', 'fun' : lambda W: sum(W)-1. })  # Budget Constraint : Sum of weights = 100%
    cons_2 = ({'type' : 'ineq', 'fun' : lambda W: W})
    bnds = [(w - threshold, w + threshold) for w in weight_mkt]  # Set weight threshold for each asset
    initialWeight = np.ones(numAsset) / numAsset  # Set initial weight as 1/N 
    
    opt = minimize(getSharpe, initialWeight, 
                   method='SLSQP',constraints=[cons_1, cons_2], bounds=bnds)
    
    x = pd.Series(opt.x, index = priceData.columns)
    x = x.sort_values(ascending=False)

    return x


def optimizedSchedule(rebalData, buffer):

    dateList = list(set(rebalData.date))
    dateList.sort()

    g = {}
    for date in dateList:  
        data = rebalData[rebalData.date == date].set_index('code')
        price = get_stock_price(data.index.values, date-datetime.timedelta(365) , date)
        w = data.loc[price.columns,:].weight.values
        optimized = maxSharpe(price, w, buffer)
        g[date] = optimized
        print(date)
    opt = pd.concat(g)
    opt.name = 'weight'
    opt = opt.reset_index()
    opt.columns = ['date', 'code' ,'weight']

    for date_, group in opt.groupby('date'):
        if len(group[~pd.isna(group['weight'])]) == 0:
            print(date_)
            codes_ = group['code'].values
            weight_ = rebalData[rebalData['date'] == date_].set_index('code').loc[codes_,'weight']
            opt.loc[group.index, 'weight'] = weight_.values
    
    return opt
