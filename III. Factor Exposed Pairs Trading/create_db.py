# -*- coding: utf-8 -*-
"""
Created on Wed May 15 10:30:17 2019

@author: Woojin
"""

import sqlite3
from sqlite3 import Error
import os
import pandas as pd
import numpy as np
import datetime
import pymysql
from sqlalchemy import create_engine

path = "C:/Woojin/###. Git/Project_Q/III. Factor Exposed Pairs Trading/"
os.chdir(path)
import util
         
# DB 만들기 (1회용)
util.create_db(path + 'rdata.db')
# db 내 테이블 만들기
# 1) 회사명 / 회사코드 테이블 (firmInfo)
sql_create_table_firmName = """ CREATE TABLE IF NOT EXISTS firmName (
                                        code text PRIMARY KEY,
                                        name text NOT NULL
                                    ); """
util.create_tables('rdata.db', sql_create_table_firmName)
conn = util.create_connection_db('rdata.db')
firmCodes = pd.read_excel('firmCode.xlsx')  # 데이터 삭제됨
firmCodes.to_sql('firmName', con=conn, if_exists='append', index=False)
conn.execute("SELECT * FROM firmName").fetchall()

sector = util.data_cleansing_ts(pd.read_excel('sector.xlsx'))  #데이터 삭제됨
sector = sector.unstack(level=1).reset_index()
sector.columns = ['code', 'date', 'sector']
sector['date'] = [''.join([x for x in str(sector['date'][i])[:10] if x != '-']) for i in range(len(sector))]
sql_create_table_firmInfo = """ CREATE TABLE IF NOT EXISTS firmInfo (
                                        code text,
                                        date text NOT NULL,
                                        sector text,
                                        PRIMARY KEY (code, date)
                                    ); """
conn = util.create_connection_db('rdata.db')
sector.to_sql('firmInfo', con = conn, if_exists = 'append', index = False)
conn.execute("SELECT * FROM firmInfo").fetchall()


price = util.get_stock_price(firmCodes, '2007-01-01', '2018-04-30')  # 가격데이터 불러오기


conn.close()

