# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 15:57:21 2019

@author: check
"""


import os, inspect
import sqlite3
import pandas as pd

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(path)

conn = sqlite3.connect('DB_ver_1.3.db')  # Database 연결 (없는 경우 자동생성)
c = conn.cursor()  # you can create a Cursor object and call its 
                   # execute() method to perform SQL commands
                   


infoData = pd.read_excel('infoData.xlsx').iloc[:,1:]
infoData = infoData.drop_duplicates()
infoData_sql = [tuple(infoData.iloc[i,:].values) for i in range(len(infoData))]


c.executemany('INSERT OR IGNORE INTO info VALUES (?,?,?,?,?,?)', infoData_sql )

conn.commit()

conn.close()                   