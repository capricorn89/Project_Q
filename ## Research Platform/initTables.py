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

import get_data as gd

# Pandas 데이터프레임 형태로 불러온 기존 데이터를 각 테이블에 삽입

# 1) Econ
oldData = pd.read_excel('sampleData.xlsx')  # 기존 데이터 불러오기 
oldData = oldData[['date', 'ticker', 'value']]
oldData.drop_duplicates(inplace=True)
oldData.reset_index(drop=True, inplace=True)
oldData['date'] = [int(oldData.date[i].replace("-","")) for i in range(len(oldData))]

conn = sqlite3.connect('DB_ver_1.1.db')  # Database 연결 (없는 경우 자동생성)
c = conn.cursor()  # you can create a Cursor object and call its 
                   # execute() method to perform SQL commands
oldData.to_sql('econ', conn, if_exists = 'replace')

# 잘 들어갔는지 확인할려면 Uncomment 후 실행
pd.read_sql_query("select * from econ;", conn)


# 2) Info (including NDOR)

ndorData = pd.read_excel('infoData.xlsx').iloc[:,1:]
ndorData.drop_duplicates(inplace=True)
ndorData.set_index('ticker', inplace = True)
ndorData.to_sql('info', conn, if_exists = 'replace')


# 잘 들어갔는지 확인할려면 Uncomment 후 실행
pd.read_sql_query("select * from info;", conn)


