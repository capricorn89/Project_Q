# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 07:55:14 2019

@author: Woojin
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


class analysis:
    
    def __init__(self, returnData_, frequency):   
    
        self.returnData = returnData_
        self.frequency = frequency
        self.names = returnData_.columns.values
        self.market = self.returnData['market']
        
        if self.frequency == 'D':
            self.power = 252
            
        elif self.frequency == 'M':
            self.power = 12
            
        elif self.frequency == 'Q':
            self.power = 4
            
        elif self.frequency == 'H':
            self.power = 2
            
        elif self.frequency == 'Y':
            self.power = 1
            
        else:
            self.power = 1

    def annTotalReturn(self, name):
        if name == 'all':
            meanReturn = self.returnData.mean()
        else:
            meanReturn = self.returnData[name].mean()
        annReturn = ((meanReturn + 1)**self.power)-1
        return annReturn
        
    def annVol(self, name):
        if name == 'all':
            vol = self.returnData.std()
        else:
            vol = self.returnData[name].std()        
        annVol = vol * np.sqrt(self.power)
        return annVol
    
    def annTE(self, name):
        if name == 'all':
            cols = list(self.names)
            cols.remove('market')
            #print(cols)
            TE = self.returnData[cols].sub(self.market, axis =0)
             
        else:
            TE = self.returnData[name] - self.market
        TE = TE.std() * np.sqrt(self.power)
        return TE    
    
    def get_cumReturnPlot(self, name):
        if name == 'all':
            data = self.returnData
        else:
            data = self.returnData[name]
        (data + 1).cumprod().plot(figsize=(12,8)) 
        
    
