# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 18:59:48 2019

@author: Woojin
"""

import os
import pandas as pd
import numpy as np
import datetime
import pymysql



def get_amt_money(weightList, totalMoney):
    '''
    weightList : array
    totalMoney : scalar
    '''    
    return weightList * totalMoney

def get_recentBday(date):
    
    date = ''.join([x for x in str(date)[:10] if x != '-'])
    db = pymysql.connect(host='192.168.1.190', port=3306, user='root', passwd='gudwls2@', db='quant_db',charset='utf8',autocommit=True)
    cursor = db.cursor()    
    sql = "SELECT DISTINCT TRD_DT FROM dg_fns_jd WHERE TRD_DT <=" + date 
    sql += " ORDER BY TRD_DT"
    cursor.execute(sql)
    data = cursor.fetchall()
    data = data[-1][0]
    db.close()
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


def get_num_stock(moneyList, priceList):
    '''
    moneyList : array
    priceList : array
    '''    
    return moneyList / priceList


def get_basket_history(stockCodes, numStock, date_start, date_end):
    basketPriceData =  get_stock_price(stockCodes, date_start, date_end).fillna(0)
    dates = basketPriceData.index
    priceHistory = basketPriceData.values.dot(numStock)    
    priceHistory = pd.DataFrame(priceHistory, index = dates)
    return priceHistory
    
def get_equalweight(stockCode):   
    return np.ones(len(stockCode)) / len(stockCode)


'''
Backtest 시행

input : 
    - dollar_inv : 초기 투입 자금
    - rebal_date : 전체 리밸런싱 스케쥴
    - stock_code : 각 리밸런싱 일자별 포지션 (종목코드)
    - stock_weight : 각 리밸런싱 일자별 비중 
    - price_data : 전체 주가 데이터

output :
    - inv_money_history : 투자금 일별 히스토리

'''

def get_backtest_history(dollar_inv, rebalData, equal_weight = False, roundup = False, tradeCost = 0.01):
    
    
    dollar_ongoing = dollar_inv
    basket_history = {}
    rebal_date = list(set(rebalData.date))
    rebal_date.sort()
    tradeCost_history = {}
    inv_money_list = [dollar_inv]   
    inv_money_date = [rebal_date[0]]
    
    for i in range(len(rebal_date)-1):
      
        rebalDate = rebal_date[i]  # 리밸런싱 시점         
        stock_list_i = rebalData[rebalData.date == rebal_date[i]].code  # 기간별 종목 리스트 
        #print(rebalDate)
        # 동일가중 / 시총가중        
        if equal_weight == True:
            w_ = get_equalweight(stock_list_i)  
        else:
            w_ = rebalData[rebalData.date == rebal_date[i]].weight

        weightData = pd.concat([stock_list_i, w_], axis = 1).set_index('code')        
            
        if i == 0:
            weightData['money'] = weightData['weight'] * dollar_inv  # 1기인 경우 초기 투자금으로 시작
        else:
            weightData['money'] = weightData['weight'] * dollar_ongoing # 1기 이후는 투자금의 매 기간별 마지막 시점에서의 금액 재투자            
        
        stock_price_ = get_stock_price(stock_list_i, get_recentBday(rebalDate), get_recentBday(rebalDate)).transpose()  # 해당 일자의 바스켓의 주가 추출       
        weightData = pd.concat([weightData, stock_price_], axis=1)
        weightData.columns = ['weight', 'money', 'price']
        weightData['n_stocks'] = weightData['money'] / weightData['price']
        
        if roundup == True: # 초기 투입자금으로 투자가능한 종목별 주식수 (정수로 내림)
            weightData.n_stocks = np.floor(weightData.n_stocks)  
        else:
            pass
            
        basket_price = get_basket_history(weightData.index.values, weightData.n_stocks.values, get_recentBday(rebal_date[i]), get_recentBday(rebal_date[i+1]))     
        dollar_ongoing = basket_price.iloc[-1,:].values[0]  # 투자금의 매 기간별 마지막 시점에서의 포트의 가치(금액)
        #print(inv_money_list)
        
        # Trading Cost 반영
        if equal_weight == True:
            pass       
        
        else:
            # 투자기간 마지막 시점 (다음리밸) 에서의 가격
            new_price = get_stock_price(weightData.index.values, get_recentBday(rebal_date[i+1]), get_recentBday(rebal_date[i+1])).transpose()  
            weightData = pd.concat([weightData, new_price], axis = 1)
            weightData.columns = ['weight', 'money', 'price', 'n_stocks', 'last_price']
            weightData['last_weight'] = (weightData.last_price * weightData.n_stocks) / dollar_ongoing  # 투자기간의 마지막 시점에서의 비중         
            weightData['new_weight'] = rebalData[rebalData.date == rebal_date[i+1]].set_index('code')['weight']  # 새로운 비중            
            tradingCost = np.abs(weightData.new_weight - weightData.last_weight).sum() * tradeCost * dollar_ongoing
            
        dollar_ongoing -= tradingCost     
        basket_history[rebalDate] = basket_price
        #print(basket_price.values.squeeze()[1:])
        inv_money_list += list(basket_price.values.squeeze()[1:-1])
       # print(inv_money_list)
        inv_money_list += [dollar_ongoing]
        inv_money_date += list(basket_price.index[1:])
    
        tradeCost_history[''.join([x for x in str(rebal_date[i+1])[:10] if x != '-'])] = tradingCost
       #last_price = last_price * n_stocks_
        #last_weight = last_price / sum(last_price)
        
    inv_money_history = pd.DataFrame(inv_money_list, index = inv_money_date) 
    
    return inv_money_history, tradeCost_history











#
#
## BM 대비 성과 측정
#def CAGR():
#    pass
#
#def sharpeRatio():
#    pass
#
#def TE(bm, fund):
#    '''
#    bm, fund : time series return
#    '''
#    te = np.std(fund-bm) * np.sqrt(252) * 100
#    return te
#    
#    
#    
#'''
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
#
#
#
#
#
#
#
#
#
#
#
#
#
#
