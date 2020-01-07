# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 15:12:37 2020

@author: Woojin
"""

import os, inspect
import pandas as pd
import platform

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(path)

# 3) 각 티커별로 데이터 매칭해서 Dictionary로 만들기
def clean_sheet(bb_sheet, bb_off = True):
    if (platform.system() == 'Windows')|(platform.system() == 'Darwin'): # Windows or mac
        tk_col = bb_sheet.iloc[2, 1:].values
        if bb_off:
            dt_idx = bb_sheet.iloc[6:, 0].values
            vals = bb_sheet.iloc[6:, 1:].values               
        else:
            dt_idx = bb_sheet.iloc[5:, 0].values
            vals = bb_sheet.iloc[5:, 1:].values
        df = pd.DataFrame(vals, index = dt_idx, columns = tk_col)
        return df
    
    else:
        print("Not Windows, Check the codes!")

def get_data():
    
    df_excel = pd.ExcelFile('ResearchData.xlsx')
    
    # 1) 경제지표 티커 불러오기
    econ_ticker = pd.read_excel(df_excel, sheet_name = 'macro')
    econ_ticker = econ_ticker[econ_ticker.Category == 'Economic Growth'][['Ticker', 'Name', 'Period']].reset_index(drop=True)
    
    # 2) 지수 데이터 불러오기
    index_ticker = pd.read_excel(df_excel, sheet_name = 'index')
    index_ticker = index_ticker[['Ticker', 'Name']].reset_index(drop=True)
    
    nameData = pd.concat([econ_ticker, index_ticker])[['Ticker', 'Name']].set_index('Ticker')
    df_q = clean_sheet(pd.read_excel(df_excel, 'bb_quarterly'))
    df_m = clean_sheet(pd.read_excel(df_excel, 'bb_monthly'))
    df_d = clean_sheet(pd.read_excel(df_excel, 'bb_daily'))
    
    econ_data = {}
    for i in range(len(econ_ticker)):
        tk = econ_ticker.Ticker[i]
        period = econ_ticker.Period[i]
        if period == 'Q':
            econ_data[tk] = df_q.loc[:, tk].dropna()
        elif period == 'M':
            econ_data[tk] = df_m.loc[:, tk].dropna()
        elif period == 'D':
            econ_data[tk] = df_d.loc[:, tk].dropna()
            
    index_data = {}
    for i in range(len(index_ticker)):
        tk = index_ticker.Ticker[i]
        index_data[tk] = df_d.loc[:, tk].dropna()
        
    return nameData, econ_data, index_data, econ_ticker, index_ticker



    