# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 15:25:52 2019

@author: Woojin
"""

# https://www.sqlitetutorial.net/sqlite-python/update/

import os, inspect
import sqlite3
import pandas as pd
import DatastreamDSWS as DSWS
from tqdm import tqdm
import datetime

####################################
# 현재 DB 접속 : Ticker들 가져오기
####################################
DBName = 'DB_ver_1.3.db'
path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(path)

######################################
# Info table에서 한국 관련 티커 추출
######################################

conn = sqlite3.connect(DBName)  # Database 연결 (없는 경우 생성까지)
qry = "SELECT DISTINCT ticker FROM info "
qry += "WHERE country = 'KOREA'"

#qry = "SELECT DISTINCT ticker FROM info"  # 한번 쓰면 삭제할것

tickers = list(pd.read_sql_query(qry, conn)['ticker'].values)
conn.close()


import DB_util as dsws

username_ = input("USERNAME : ")
pw_ = input("PASSWORD : ") 

conn = sqlite3.connect(DBName)  # Database 연결 (없는 경우 생성까지)
cur = conn.cursor()

ds = dsws.get_DSWS(username = username_, password = pw_)
day_start = "2000-01-01"


data_dict = {}

for i in tqdm(range(len(tickers))):    
    dsData = ds.econData(tickers[i], day_start = day_start)   # 지표
    if type(dsData) == 'Dataframe':
        data_dict[tickers[i]] = dsData
    
conn.close()