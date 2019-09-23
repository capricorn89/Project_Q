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
import datetime

####################################
# 현재 DB 접속 : Ticker들 가져오기
####################################
DBName = 'DB_ver_1.2.db'
cpath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(path)

######################################
# Datastream 연결 
######################################


######################################
# Info table에서 최근 일주일간 데이터 업데이트 예정이었던 티커 추출 
######################################
day_update_from = pd.datetime.today() - datetime.timedelta(10)
day_update_to = pd.datetime.today() + datetime.timedelta(10)

day_update_from = pd.datetime.strftime(day_update_from, "%Y%m%d")
day_update_to = pd.datetime.strftime(day_update_to, "%Y%m%d")

conn = sqlite3.connect(DBName)  # Database 연결 (없는 경우 생성까지)
qry = "SELECT ticker, NDOR_date FROM info "
qry += "WHERE NDOR_date BETWEEN " + day_update_from + " AND " + day_update_to 
tickers = list(pd.read_sql_query(qry, conn)['ticker'].values)
conn.close()

##################################
# 기존 데이터의 업데이트 
##################################

import get_data as dsws
username_ = input("USERNAME : ")
pw_ = input("PASSWORD : ") 
ds = dsws.get_DSWS(username = username_, password = pw_)
day_start = pd.datetime.strftime(pd.datetime.today() - datetime.timedelta(40), "%Y-%m-%d")
newData = []
metaData = {}
for i in tqdm(range(len(tickers))):
    dsData = ds.econData(tickers[i], day_start = day_start)   # 지표
    if dsData == 'Not Available':
        print(tickers[i], 'Not available')
    else:
        newData.append(dsData)
        
    dsMeta = ds.NDOR(tickers[i])  # 메타 정보 -> 걍 모든 티커를 다 하는게 나은지는 고민 필요
    if dsMeta == 'Not Available':
        print(tickers[i], 'Not available')
    else:
        metaData[tickers[i]] = dsMeta
        '''
        바로 info 에 업데이트
        UPDATE info
        SET NDOR_date = dsMeta[0], NDOR_time = dsMeta[1], NDOR_ref_date = dsMeta[2]
        WHERE ticker = tickers[i]
        '''
        
final_df = pd.concat(newData,axis=1).stack().reset_index()
final_df.columns = ['date', 'ticker', 'value']
final_df.set_index('date', inplace=True)
final_df = final_df.sort_values('date')
final_df.index = [int(final_df.index[i].replace("-","")) for i in range(len(final_df))]

conn = sqlite3.connect(DBName)
final_df.to_sql('econ', conn, if_exists = 'replace')
conn.close()

#
sampleData = pd.read_sql_query("select * from econ ", conn)  # test용 쿼리
#
    
    

