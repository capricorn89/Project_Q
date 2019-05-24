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
from tqdm import tqdm
import util
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
    turnover_history = {}
    inv_money_list = [dollar_inv]   
    inv_money_date = [rebal_date[0]]
    
    for i in tqdm(range(len(rebal_date)-1)):
      
        rebalDate = rebal_date[i]  # 리밸런싱 시점         
        stock_list_i = rebalData[rebalData.date == rebal_date[i]].code  # 기간별 종목 리스트 
        #print(rebalDate)
        # 동일가중 / 시총가중        
        if equal_weight == True:
            w_ = util.get_equalweight(stock_list_i)  
        else:
            w_ = rebalData[rebalData.date == rebal_date[i]].weight

        weightData = pd.concat([stock_list_i, w_], axis = 1).set_index('code')        
            
        if i == 0:
            weightData['money'] = weightData['weight'] * dollar_inv  # 1기인 경우 초기 투자금으로 시작
        else:
            weightData['money'] = weightData['weight'] * dollar_ongoing # 1기 이후는 투자금의 매 기간별 마지막 시점에서의 금액 재투자            
        
        stock_price_ = util.get_stock_price(stock_list_i, 
                                            util.get_recentBday(rebalDate),
                                            util.get_recentBday(rebalDate)).transpose()  # 해당 일자의 바스켓의 주가 추출     
        
        weightData = pd.merge(weightData, stock_price_, how = 'inner', 
                              left_on= weightData.index,
                              right_on = stock_price_.index).set_index('key_0')
        
        weightData.columns = ['weight', 'money', 'price']
        weightData['n_stocks'] = weightData['money'] / weightData['price']
        
        if roundup == True: # 초기 투입자금으로 투자가능한 종목별 주식수 (정수로 내림)
            weightData.n_stocks = np.floor(weightData.n_stocks)  
        else:
            weightData.n_stocks = weightData.n_stocks
            
        basket_price = util.get_basket_history(weightData.index.values, 
                                               weightData.n_stocks.values, 
                                               util.get_recentBday(rebal_date[i]), 
                                               util.get_recentBday(rebal_date[i+1]))     
        
        dollar_ongoing = basket_price.iloc[-1,:].values[0]  # 투자금의 매 기간별 마지막 시점에서의 포트의 가치(금액)
        #print(inv_money_list)
        
        # Trading Cost 반영
        if equal_weight == True:
            pass       
        
        else:
            # 투자기간 마지막 시점 (다음리밸) 에서의 가격
            last_price = util.get_stock_price(weightData.index.values, 
                                              util.get_recentBday(rebal_date[i+1]), 
                                              util.get_recentBday(rebal_date[i+1])).transpose()  
            
            weightData = pd.merge(weightData, last_price, how = 'inner', 
                                  left_on = weightData.index, 
                                  right_on = last_price.index).set_index('key_0')
            
            weightData.columns = ['weight', 'money', 'price', 'n_stocks', 'last_price']
            
            weightData['last_weight'] = (weightData.last_price * weightData.n_stocks) / (weightData.last_price * weightData.n_stocks).sum()  # 투자기간의 마지막 시점에서의 비중         
            weightData['new_weight'] = rebalData[rebalData.date == rebal_date[i+1]].set_index('code')['weight']  # 새로운 비중            
            tradingCost = np.abs(weightData.new_weight - weightData.last_weight).sum() * tradeCost * dollar_ongoing
            
        dollar_ongoing -= tradingCost     
        basket_history[rebalDate] = basket_price
        inv_money_list += list(basket_price.values.squeeze()[1:-1])
        inv_money_list += [dollar_ongoing]
        inv_money_date += list(basket_price.index[1:])  
        tradeCost_history[''.join([x for x in str(rebal_date[i+1])[:10] if x != '-'])] = tradingCost
        turnover_history[''.join([x for x in str(rebal_date[i+1])[:10] if x != '-'])] = tradingCost / dollar_ongoing
        
    inv_money_history = pd.DataFrame(inv_money_list, index = inv_money_date) 
    
    return inv_money_history, tradeCost_history, turnover_history

