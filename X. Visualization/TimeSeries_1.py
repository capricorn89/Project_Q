# -*- coding: utf-8 -*-
"""
Created on Tue Jun 18 09:39:26 2019

@author: user
"""

from bokeh.plotting import figure, output_file, show
from bokeh.layouts import column
from bokeh.models import LinearAxis, Range1d
import numpy as np
import pandas as pd
import os
import numpy as np

#import holoviews as hv
#from holoviews import opts

#os.chdir('C:/Woojin/###. Git/Project_Q/X. Visualization')
os.chdir('/Users/Woojin/Documents/GitHub/Project_Q/X. Visualization')         
ts = pd.read_excel('test_timeseries.xlsx', sheet_name = 'Sheet1')

# output to static HTML file
#output_file("lines.html")

# create a new plot with a title and axis labels

def drawdown(Series):    
    dd_Series = []
    prev_high = 0
    for i in range(len(Series)):
        if i == 0:
            prev_high = 0
            dd = 0
            dd_Series.append(dd)
        else:
            prev_high = max(Series[:i])            
            if prev_high > Series[i]:                
                dd = Series[i] - prev_high
                #print(dd)
            else:
                prev_high = Series[i]
                dd = 0                
            #print(prev_high)

            dd_Series.append(dd)
    dd_Series = pd.Series(dd_Series)    
    return dd_Series


x = ts.index
y1 = ( ts['long'].pct_change().fillna(0) + 1 ).cumprod() * 100
y2 = ( ts['I.101'].pct_change().fillna(0) + 1 ).cumprod() * 100

## add a line renderer with legend and line thickness
p1 = figure(plot_width=800, plot_height=300, title="simple line example",
           x_axis_type="datetime", x_axis_label='Date', y_axis_label='Price')

p1.line(x, y1, legend="Portfolio", color = 'red', line_width=2)
p1.line(x, y2, legend="KOSPI200", color = 'grey', line_width=2)
p1.legend.location = "top_left"
p1.legend.click_policy="hide"

p2 = figure(plot_width=800, plot_height=100, x_range = p1.x_range, x_axis_type="datetime")
p2.varea(x=x , y1 = np.zeros(len(y1)), y2 = drawdown(y1))

show(column(p1,p2))



# 각 지표별로 3MA, 12MA 계산하고 그에 따라 국면 부여 (R, E, S, C)
# 지표를 부여된 국면에 따라 서로 다른 색으로 표시되도록 Plot

from bokeh.transform import factor_cmap
from bokeh.palettes import Spectral10
from bokeh.models import ColumnDataSource

macroData = pd.read_excel('test_timeseries.xlsx', sheet_name = 'macro').set_index('Date')[['OECD_CLI','ESI']]
numIndicator = len(macroData.columns)

for i in range(numIndicator):
    colName_3 = macroData.columns[i] + '_3MA'
    colName_12 = macroData.columns[i] + '_12MA'
    colName_ind = macroData.columns[i] + '_indic'
    
    macroData[colName_3] = macroData[macroData.columns[i]].rolling(3).mean() # 과거 3개월 평균
    macroData[colName_12] = macroData[macroData.columns[i]].rolling(12).mean()  # 과거 12개월 평균
    macroData[colName_ind] = np.nan
    macroData[colName_ind] = (macroData[colName_ind]).astype(str)  # 빈 칼럼에 String을 집어넣으면 에러 발생해서 임시 방편으로..
    
    for j in range(len(macroData)-1):  # 처음 데이터는 비교 대상 없으므로 제외
        
        if (macroData[colName_3][j+1] < macroData[colName_12][j+1]) and (macroData[colName_3][j+1] >= macroData[colName_3][j]):
            macroData[colName_ind][j+1] = str('Recovery')  # Recovery
            
        elif (macroData[colName_3][j+1] >= macroData[colName_12][j+1]) and (macroData[colName_3][j+1] >= macroData[colName_3][j]):
            macroData[colName_ind][j+1] = str('Expansion')  # Expansion
    
        elif (macroData[colName_3][j+1] >= macroData[colName_12][j+1]) and (macroData[colName_3][j+1] < macroData[colName_3][j]):
            macroData[colName_ind][j+1] = str('Slowdown')  # Slowdown

        elif (macroData[colName_3][j+1] < macroData[colName_12][j+1]) and (macroData[colName_3][j+1] < macroData[colName_3][j]):
            macroData[colName_ind][j+1] = str('Contraction')  # Contraction
            
        else:
            continue

regimes = ['Recovery','Expansion','Slowdown','Contraction']

source = ColumnDataSource(data = dict(date= macroData.index.values[12:],
                                      value= list(macroData['ESI'].values[12:]),
                                      regime = list(macroData['ESI_indic'].values[12:])))

p1 = figure(plot_width=800, plot_height=300, title="simple line example",
           x_axis_type="datetime", x_axis_label='Date', y_axis_label='Level')        

p1.scatter("date", "value", source = source, legend = "regime",                    
           color = factor_cmap("regime" ,palette=Spectral10, factors= regimes))        
p1.legend.location = "top_left"
show(p1)    