# -*- coding: utf-8 -*-
"""
Created on Tue May 21 16:38:33 2019

@author: user
"""
import os
import pandas as pd
import numpy as np
import datetime
import pymysql
import sqlite3
from sqlite3 import Error
import calendar

def get_stock_price(stockCodes, date_start, date_end):
    '''
    input: 
    - stockcodes : list
    - date_start : datetime
    - date_end : datetiem
    
    output : DataFrame
    - Index : Stock code
    - value : Stock Price
    '''
    date_start = ''.join([x for x in str(date_start)[:10] if x != '-'])
    date_end = ''.join([x for x in str(date_end)[:10] if x != '-'])
    db = pymysql.connect(host='192.168.1.190', port=3306, user='root', passwd='gudwls2@', db='quant_db',charset='utf8',autocommit=True)
    cursor = db.cursor()
    joined = "\',\'".join(stockCodes)
    sql = "SELECT GICODE, TRD_DT, ADJ_PRC FROM dg_fns_jd WHERE TRD_DT BETWEEN " + date_start
    sql += ' AND ' + date_end
    sql += (" AND GICODE IN (\'" + joined + "\')")
    cursor.execute(sql)
    data = cursor.fetchall()
#    data = pd.DataFrame(list(data))
#    data = data.pivot(index = 1, columns = 0, values = 2)
#    data.index = pd.to_datetime(data.index.values)
#    db.close()   
    return data


def create_db(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        conn.close()

def create_connection_db(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)

        return conn
    except Error as e:
        print(e)
 
    return None

def create_table(conn, query):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(query)
    except Error as e:
        print(e)


def create_tables(db_file, query):    
    # create a database connection
    conn_ = create_connection_db(db_file)
    if conn_ is not None:
        create_table(conn_, query)
    else:
        print("Error! cannot create the database connection.")        


def data_cleansing(rawData):
    
    firmCode = rawData.iloc[7,5:].values
    yearIndex = [int(str(x)[:4]) for x in rawData.iloc[10:,1].values]
    monthIndex = [int(str(x)[4:]) for x in rawData.iloc[10:,1].values]
    newDateIndex = []
    for i in range(len(yearIndex)):
        days = calendar.monthrange(yearIndex[i], monthIndex[i])[1]
        newDateIndex.append(datetime.datetime(yearIndex[i], monthIndex[i], days))
    
    newData = rawData.iloc[10:,5:]
    newData.columns = firmCode
    newData.index = newDateIndex
    
    return newData

def data_cleansing_ts(rawData):
    
    firmCode = rawData.iloc[6, 1:].values
    dateIndex = rawData.iloc[13:, 0].values
    newData = rawData.iloc[13:,1:]
    newData.columns = firmCode
    newData.index = dateIndex
    return newData

