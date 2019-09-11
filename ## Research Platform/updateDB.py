# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 15:25:52 2019

@author: Woojin
"""


import os, inspect
import sqlite3
import pandas as pd

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(path)

conn = sqlite3.connect('DB_example.db')  # Database 연결 (없는 경우 생성까지)
c = conn.cursor()  # you can create a Cursor object and call its 
                   # execute() method to perform SQL commands

#################################
# 기존 데이터의 업데이트 (내용 변경의 가능성이 있으니)
def update_value(ticker_, date_, val):
    conn = sqlite3.connect('DB_example.db')  # Database 연결 (없는 경우 생성까지)
    c = conn.cursor()  
    query = "UPDATE econ SET value="
    query += str(val)
    query += " where date='"
    query += str(date_)
    query += "' and ticker='"
    query += str(ticker_)
    query += "'"
    c.execute(query)
    conn.commit()

pd.read_sql_query("select * from econ where date='2019-08-30' and ticker='IDPRATE.'", conn)  # test용 쿼리

# 1. NDOR date 데이터 보고 최근 +- 7일 기준으로 업데이트 예정이었던 티커만 필터링
    
    

