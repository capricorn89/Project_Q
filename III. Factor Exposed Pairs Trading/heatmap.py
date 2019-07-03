# -*- coding: utf-8 -*-
"""
Created on Wed Jul  3 14:48:42 2019

@author: Woojin
"""

# Plot Heatmap of cointegration

import os
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib as mpl
mpl.style.use('bmh')
import pandas_datareader.data as web
import matplotlib.pylab as plt
from datetime import datetime
import statsmodels.api as sm
from pykalman import KalmanFilter
from math import sqrt

path = 'C:/Woojin/###. Git/Project_Q/III. Factor Exposed Pairs Trading'
os.chdir(path)

#import backtest_pipeline_ver2 as bt
import util

codes = pd.read_excel('firmCode.xlsx')['Code'].values
price = util.get_stock_price(codes, pd.to_datetime('2010-01-01'), pd.to_datetime('2019-06-30'))
size = util.data_cleansing(pd.read_excel('firmSize.xlsx'))

#NOTE CRITICAL LEVEL HAS BEEN SET TO 5% FOR COINTEGRATION TEST
def find_cointegrated_pairs(dataframe, critial_level = 0.05):
    n = dataframe.shape[1] # the length of dateframe
    pvalue_matrix = np.ones((n, n)) # initialize the matrix of p
    keys = dataframe.columns # get the column names
    pairs = [] # initilize the list for cointegration
    for i in range(n):
        for j in range(i+1, n): # for j bigger than i
            stock1 = dataframe[keys[i]] # obtain the price of "stock1"
            stock2 = dataframe[keys[j]]# obtain the price of "stock2"
            result = sm.tsa.stattools.coint(stock1, stock2) # get conintegration
            pvalue = result[1] # get the pvalue
            pvalue_matrix[i, j] = pvalue
            if pvalue < critial_level: # if p-value less than the critical level
                pairs.append((keys[i], keys[j], pvalue)) # record the contract with that p-value
    return pvalue_matrix, pairs

size_2018 = size.loc[:'2018-01-01'].tail(1).transpose()
kospi_midlarge = size_2018[(size_2018 >= 1) & (size_2018 <=2)].dropna().index.values
price_2018 = price.loc[:,kospi_midlarge[:10]].dropna(axis=1)  # 2018년 동안 수익이 있는 데이터만 수집

pval_matrix, pairs = find_cointegrated_pairs(price_2018)

#convert our matrix of stored results into a DataFrame
pvalue_matrix_df = pd.DataFrame(pval_matrix)

#use Seaborn to plot a heatmap of our results matrix
fig, ax = plt.subplots(figsize=(15,10))
sns.heatmap(pvalue_matrix_df,xticklabels=price_2018.columns.values ,yticklabels=price_2018.columns.values,ax=ax)