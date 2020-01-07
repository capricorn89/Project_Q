# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 15:25:52 2019

@author: Woojin
"""

##  Eikon 사용 가능 티커 정리

import eikon as ek
import pandas as pd
import numpy as np
import os, inspect
import DatastreamDSWS as DSWS
import arrow
import sqlite3
import warnings
#from arrow.factory import *
#warnings.simplefilter("ignore", ArrowParseWarning)

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(path)

class get_DSWS:
    
    def __init__(self, username, password):
        
        self.username_ = username
        self.password_ = password
        self.ds = DSWS.Datastream(username = self.username_, password = self.password_)

    def econData(self, tk, day_start):       
        df = self.ds.get_data(tickers = tk, fields = ['X'], start=day_start)
        if df.shape[1] == 3:
#            print(tk, 'Not available')
            return 'Not Available'
        else:
            df.columns = [tk]
        
        return df
    
    def NDOR(self, tk):
        '''
        Next Date of Release 의 경우 티커 하나씩 넣어야되고 여러 티커 넣음 두번째 티커부터 N/A가 나옴
        한국표준시 (도쿄 시간)로 변경 후 출력
        
        출력양식 : 날짜 (정수, 20190510), 시간 (정수, 2130)
        
        '''
        df = self.ds.get_data(tickers = tk, fields=["DS.NDOR1"])
        date_val = df[df['Datatype'] == 'DS.NDOR1_DATE']['Value'].values[0]
        time_val = df[df['Datatype'] == 'DS.NDOR1_TIME_GMT']['Value'].values[0]
        ref_date_val = df[df['Datatype'] == 'DS.NDOR1_REF_PERIOD']['Value'].values[0]
              
        if time_val != 'NA':
            seoul_dt = arrow.get(date_val + ' ' + time_val, 'YYYY-MM-DD HH:mm').to('Asia/Tokyo').datetime
            seoul_date = int(pd.to_datetime(seoul_dt).strftime("%Y%m%d"))
            seoul_time = int(pd.to_datetime(seoul_dt).strftime("%H%M"))  
    
        elif (time_val == 'NA') & (date_val != 'NA'):
            seoul_dt = arrow.get(date_val + ' ' + time_val, 'YYYY-MM-DD').datetime
            seoul_date = int(pd.to_datetime(seoul_dt).strftime("%Y%m%d"))
            seoul_time = np.NaN  
            
        elif (time_val == 'NA') & (date_val == 'NA'):
            seoul_date = np.NaN
            seoul_time = np.NaN  
        
        else:
            return 'Not Available'
    
        if ref_date_val != 'NA':
            ref_date_val = int(str(ref_date_val[:4])+str(ref_date_val[5:7])+ref_date_val[8:10])
        else:
            ref_date_val = np.NaN
        
        return seoul_date, seoul_time, ref_date_val
    
    def GEOGN(self, tk):
        '''
        GEOGN - Geographical Classification Of Company (Name)
        '''           
        df = self.ds.get_data(tickers = tk, fields=["GEOGN"], kind = 0)
        geo = df['Value'].values        
        return geo
    
    
class get_DB:
    
    def __init__(self, DBName):
        
        self.DBName_ = DBName
        
    def from_econ(self, ticker, startDate, endDate):
        
        '''
        ticker : econ 에 저장된 ticker
        startDate : int, "YYYYMMDD"
        endDate : int, "YYYYMMDD"
        '''
        conn = sqlite3.connect(self.DBName_)
        qry = "SELECT ticker, date, value FROM econ"
        qry += " WHERE ticker in " + str(tuple(ticker))
        qry += " AND (date BETWEEN "
        qry += "'" + str(startDate) + "'"
        qry += " AND "
        qry += "'" + str(endDate) + "')"
        df = pd.read_sql_query(qry, conn)
        
        df['date'] = [pd.datetime.strptime(str(df['date'][i]), "%Y%m%d") for i in range(len(df))]
        df = df.pivot(index = 'date', columns = 'ticker', values = 'value')
        
        return df
    
    
    def from_info(self, ticker):
        
        '''
        ticker : econ 에 저장된 ticker
        '''
        conn = sqlite3.connect(self.DBName_)
        qry = "SELECT * FROM info"
        qry += " WHERE ticker ="
        qry += " '" + ticker + "'"
        df = pd.read_sql_query(qry, conn)
        
        return df        
        

    
