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
import backtest_pipeline as bt
import util

os.chdir('C:/Woojin/###. Git/Project_Q/IV. Factor Model Test Bed')

start_year, start_month = 2006, 12
start_day = calendar.monthrange(start_year, start_month)[1]
end_year, end_month = 2019, 3
end_day = calendar.monthrange(end_year, end_month)[1]

start_invest = pd.datetime(start_year, start_month, start_day)
end_invest = pd.datetime(end_year, end_month, end_day)
rebal_sche = pd.date_range(start_invest, end_invest, freq = 'M')

marketInfo = util.data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'market'))
mktcap = util.data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'mktcap'))
risk_1 = util.data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'risk_1'))
risk_2 = util.data_cleansing_ts(pd.read_excel('testData.xlsx', sheet_name = 'risk_2'))

factor_PSR = util.data_cleansing(pd.read_excel('testData.xlsx', sheet_name = 'sales'))
factor_PBR = util.data_cleansing(pd.read_excel('testData.xlsx', sheet_name = 'book'))
factor_PER = util.data_cleansing(pd.read_excel('testData.xlsx', sheet_name = 'earnings'))