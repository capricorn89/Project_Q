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

conn = sqlite3.connect('DB_ver_1.1.db')  # Database 연결 (없는 경우 자동생성)
c = conn.cursor()  # you can create a Cursor object and call its 
                   # execute() method to perform SQL commands

#################################
# 지표별 Value 데이터를 넣기 위한 'econ' 테이블 및  ticker별 정보를 넣기 위한 'info' 테이블 생성
c.execute('''CREATE TABLE IF NOT EXISTS econ(
                    date INTEGER,
                    ticker text, 
                    value real,
                    PRIMARY KEY (date, ticker))''')

'''
<예시>

table명 : econ
_____________________________________
date        |  ticker     |  value
_____________________________________
20190101  |  USFEFRL    |  2.00
-------------------------------------
20190101  |  USFEFRH    |  2.25
-------------------------------------
20190201  |  USFEFRL    |  2.25
-------------------------------------
'''

create_info = '''CREATE TABLE IF NOT EXISTS info(
                        ticker TEXT PRIMARY KEY, 
                        name TEXT,
                        country TEXT,
                        NDOR_date INTEGER,
                        NDOR_time INTEGER
                        NDOR_ref_date INTEGER)'''
# ticker : PK, 티커
# name : 종목명 / 지표명
# country :국가명
# NDOR_date : 다음 발표 예정일 (YYYYMMDD)
# NDOR_time : 다음 발표 예정시각 (HHMM)
c.execute(create_info)
conn.close()