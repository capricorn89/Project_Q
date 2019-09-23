# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 15:25:52 2019

@author: Woojin
"""


import os, inspect
import sqlite3
import pandas as pd
import DatastreamDSWS as DSWS
from tqdm import tqdm

####################################
# 현재 DB 접속 : Ticker들 가져오기
####################################
DBName = 'DB_ver_1.2.db'
path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(path)

conn = sqlite3.connect(DBName)  # Database 연결 (없는 경우 생성까지)
c = conn.cursor()  # you can create a Cursor object and call its 
                   # execute() method to perform SQL commands

tickers = list(pd.read_sql_query("select ticker from info", conn)['ticker'].values)
conn.close()


######################################
# Datastream 연결 후 데이터 다운로드
######################################
import get_data as dsws
username_ = input("USERNAME : ")
pw_ = input("PASSWORD : ") 
ds = dsws.getData(username = username_, password = pw_)

newData = []
metaData = {}
for i in tqdm(range(len(tickers))):
    dsData = ds.econData(tickers[i], day_start = '2015-01-01')   # 지표
    if dsData.shape[1] == 3:
        print(tickers[i], 'Not available')
        pass
    else:
        newData.append(dsData)
    dsMeta = ds.NDOR(tickers[i])  # 메타 정보
    metaData[tickers[i]] = dsMeta
final_df = pd.concat(newData,axis=1).stack().reset_index()
final_df.columns = ['date', 'ticker', 'value']
final_df.set_index('date', inplace=True)
final_df = final_df.sort_values('date')
final_df.index = [int(final_df.index[i].replace("-","")) for i in range(len(final_df))]



##################################
# 기존 데이터의 업데이트 (내용 변경의 가능성이 있으니)
##################################

def update_value(newdata, table):
    conn = sqlite3.connect(DBName)  # Database 연결 (없는 경우 생성까지)
    newdata.to_sql(table, conn, if_exists = 'replace')
    conn.close()
    
update_value(final_df, 'econ')

#
sampleData = pd.read_sql_query("select * from econ ", conn)  # test용 쿼리
#
## 1. NDOR date 데이터 보고 최근 +- 7일 기준으로 업데이트 예정이었던 티커만 필터링
    
    

