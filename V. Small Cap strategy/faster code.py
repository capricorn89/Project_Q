# -*- coding: utf-8 -*-
"""
Created on Fri Jun 28 14:42:43 2019

@author: user
"""

import os
import pandas as pd
import numpy as np
import datetime
import pickle

path = 'C:/Woojin/###. Git/Project_Q/V. Small Cap strategy/Data'
os.chdir(path)

#%%

total = pd.read_excel(filename, None)

sheets = list(total.keys())

data = []
for name in sheets:
    temp = [name]
    this = total[name]
    temp.append(this.iloc[9,4])
    
    this.columns = this.iloc[7]
    this.index = this.iloc[:,1]
    this = this.iloc[10:,5:]
    temp.append(this)

    data.append(temp)
#%%
filename = 'data.xlsx'
with open(filename[:filename.find('.')]+'.pkl', 'wb') as f:
    pickle.dump(data, f)
  

#%%
data = pd.read_pickle('data.pkl')

port = data[0][2] == 3  # 소형주만
port = port & (data[1][2] >= 1000)  # 시총

port = port & (data[3][2] >= 3)  # 거래대금
port = port & (data[4][2] == 0)  # 거래정지 X
port = port & (data[5][2] == 0)  # 관리종목 X
port = port & (data[6][2] == 0)  # 투자유의 X

port = port & (data[7][2] < 20)  # per 20미만

|#%%

for i in range(len(port)):
    
    dt = port.index[i]
    
    port_i = port.loc[dt,:]
    port_i = port_i[port_i == True].dropna()
    print(port_i.index)
    