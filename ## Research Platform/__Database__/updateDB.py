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
# Info table에서 최근 일주일간 데이터 업데이트 예정이었던 티커 추출 
######################################
day_update_from = pd.datetime.today() - datetime.timedelta(60)
day_update_to = pd.datetime.today() + datetime.timedelta(10)

day_update_from = pd.datetime.strftime(day_update_from, "%Y%m%d")
day_update_to = pd.datetime.strftime(day_update_to, "%Y%m%d")

conn = sqlite3.connect(DBName)  # Database 연결 (없는 경우 생성까지)
qry = "SELECT ticker, NDOR_date FROM info "
qry += "WHERE NDOR_date BETWEEN " + day_update_from + " AND " + day_update_to 

qry = "SELECT DISTINCT ticker FROM info"  # 한번 쓰면 삭제할것

tickers = list(pd.read_sql_query(qry, conn)['ticker'].values)
conn.close()

##################################
# 기존 데이터의 업데이트 
##################################

def update_info(conn, task):
    """
    update priority, begin_date, and end date of a task
    :param conn:
    :param task:
    :return: project id
    """
    sql = ''' UPDATE info
              SET NDOR_date = ? ,
                  NDOR_time = ? ,
                  NDOR_ref_date = ?
              WHERE ticker = ?'''
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()

import DB_util as dsws

username_ = input("USERNAME : ")
pw_ = input("PASSWORD : ") 

conn = sqlite3.connect(DBName)  # Database 연결 (없는 경우 생성까지)
cur = conn.cursor()

ds = dsws.get_DSWS(username = username_, password = pw_)
day_start = pd.datetime.strftime(pd.datetime.today() - datetime.timedelta(90), "%Y-%m-%d")
for i in tqdm(range(len(tickers))):
    
    dsData = ds.econData(tickers[i], day_start = day_start)   # 지표
    if type(dsData) == str:
        if dsData == 'Not Available':
            pass
    else:
        dsData = dsData.reset_index()
        dsData.columns = ['date', 'value']      
        dsData = dsData.dropna()
        dsData = dsData.reset_index(drop=True)
        
        if len(dsData) >=1 :

            for j in range(len(dsData)):
                
                dt = int(dsData.loc[j, 'date'].replace("-",""))
                vl = dsData.loc[j, 'value']
                     
                qry = "INSERT OR IGNORE INTO econ (date, ticker, value) VALUES "
                qry += "(" + str(dt) + ", " 
                qry += "'" + str(tickers[i]) + "', " + str(vl) + ")"
    
                qry_2 = "UPDATE econ SET value = " + str(vl)
                qry_2 += " WHERE date = " + str(dt)
                qry_2 += " AND ticker = '" + str(tickers[i]) + "'"
                
                cur.execute(qry)
                cur.execute(qry_2)                
                conn.commit()
            
        else:
            pass
        
    dsMeta = ds.NDOR(tickers[i])  # 메타 정보 -> 걍 모든 티커를 다 하는게 나은지는 고민 필요
    if dsMeta == 'Not Available':
        pass
    
    else:
        dsMeta = list(dsMeta)
        dsMeta.append(tickers[i])
        dsMeta = tuple(dsMeta)
        
        with conn:
            update_info(conn, dsMeta)
        pass
     
conn.close()


## For test
#conn = sqlite3.connect(DBName)
#sampleData = pd.read_sql_query("select * from econ ", conn) 
##

