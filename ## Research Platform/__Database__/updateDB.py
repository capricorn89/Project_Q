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
DBName = 'DB_ver_1.1.db'
path = os.path.dirname(os.path.ZHabspath(inspect.getfile(inspect.currentframe())))
os.chdir(path)

conn = sqlite3.connect(DBName)  # Database 연결 (없는 경우 생성까지)
c = conn.cursor()  # you can create a Cursor object and call its 
                   # execute() method to perform SQL commands

tickers = list(pd.read_sql_query("select ticker from info", conn)['ticker'].values)
conn.close()


######################################
# Datastream 연결 후 데이터 다운로드
######################################
username_ = input("USERNAME : ")
pw_ = input("PASSWORD : ") 
ds = DSWS.Datastream(username = username_, password = pw_)

newData = {}
metaData = {}
for i in tqdm(range(len(tickers))):
    #print(tickers[i])
    dsData = ds.get_data(tickers[i], start = '2015-01-01')   # 지표
    dsMeta = ds.get_data(tickers[i], fields = ['DS.NDOR1'])  # 메타 정보
    newData[tickers[i]] = dsData
    metaData[tickers[i]] = dsMeta

##################################
# 기존 데이터의 업데이트 (내용 변경의 가능성이 있으니)
##################################

def update_value(newdata):

    conn = sqlite3.connect(DBName)  # Database 연결 (없는 경우 생성까지)
    c = conn.cursor()  
    
    for i in tqdm(range(len(newdata))):
        
        tker = list(newdata.keys())[i]
        newdf = newdata[tker]
        if newdf.shape[1] != 1:
            pass
        else:
            
            for j in range(len(newdf)):
            
                date_ = newdf.index[j]
                val_ = newdf.iloc[j,:].values[0]
                           
                query = "INSERT INTO econ (date, ticker, value) "
                query += "VALUES "
                query += "('" + str(date_) + "',"
                query += "'" + str(tker) + "',"
                query += "'" + str(val_) + "');"
                
                c.execute(query)
                conn.commit()
    
    conn.close()
    
update_value(newData)

#
sampleData = pd.read_sql_query("select * from econ ", conn)  # test용 쿼리
#
## 1. NDOR date 데이터 보고 최근 +- 7일 기준으로 업데이트 예정이었던 티커만 필터링
    
    

