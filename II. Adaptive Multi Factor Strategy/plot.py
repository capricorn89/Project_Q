# -*- coding: utf-8 -*-
"""
Created on Wed Jun 19 07:29:57 2019

@author: Woojin
"""

import pandas as pd
import os
import numpy as np
import pymysql
from tqdm import tqdm
import calendar
from scipy import stats
from bokeh.plotting import figure, output_file, show
from bokeh.layouts import column
from bokeh.models import LinearAxis, Range1d
from bokeh.transform import factor_cmap
from bokeh.palettes import Spectral10
from bokeh.models import ColumnDataSource
from bokeh.models import HoverTool
from bokeh.models.widgets import Button, TextInput
from bokeh.plotting import ColumnDataSource, figure
from bokeh.models.widgets import Tabs, Panel, DateRangeSlider
from bokeh.transform import factor_cmap
from bokeh.palettes import Spectral10
from datetime import datetime
from bokeh.io import curdoc


path = 'C:/Woojin/###. Git/Project_Q/II. Adaptive Multi Factor Strategy/'
os.chdir(path)
import backtest_pipeline_ver2 as bt
import util

##########################
# 매크로 지표 그림
##########################
os.chdir(path + '/Data')
macroData = pd.read_excel('macroData.xlsx', sheet_name = 'macro').set_index('Date')[['OECD_CLI','ESI']]
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
        
        dindex = macroData.index[j+1]
        
        if (macroData[colName_3][j+1] < macroData[colName_12][j+1]) and (macroData[colName_3][j+1] >= macroData[colName_3][j]):
            macroData.loc[dindex, colName_ind] = str('Recovery')  # Recovery
            
        elif (macroData[colName_3][j+1] >= macroData[colName_12][j+1]) and (macroData[colName_3][j+1] >= macroData[colName_3][j]):
            macroData.loc[dindex, colName_ind]  = str('Expansion')  # Expansion
    
        elif (macroData[colName_3][j+1] >= macroData[colName_12][j+1]) and (macroData[colName_3][j+1] < macroData[colName_3][j]):
            macroData.loc[dindex, colName_ind]  = str('Slowdown')  # Slowdown

        elif (macroData[colName_3][j+1] < macroData[colName_12][j+1]) and (macroData[colName_3][j+1] < macroData[colName_3][j]):
            macroData.loc[dindex, colName_ind]  = str('Contraction')  # Contraction
            
        else:
            continue

regimes = ['Recovery','Expansion','Slowdown','Contraction']

source = ColumnDataSource(data = dict(date= macroData.index[12:],
                                      value= list(macroData['ESI'].values[12:]),
                                      regime = list(macroData['ESI_indic'].values[12:])))
p3 = figure(plot_width=1600, plot_height=600, title="Macro Regime Indicator (ESI)",
           x_axis_type="datetime", x_axis_label='Date', y_axis_label='Level')        
p3.scatter("date", "value", source = source, legend = "regime",                    
           color = factor_cmap("regime" ,palette=Spectral10, factors= regimes))      
p3.add_tools(HoverTool(
                tooltips=[('Date','@date{%F}'),('Level','@value{%0.1f}'), ('Regime', '@regime')],
                formatters = {'date':'datetime', 'value' :'printf'},
                mode = 'vline',
                show_arrow=False, point_policy='follow_mouse'))  
p3.legend.location = "top_left"
panel_2 = Panel(child = p3, title = 'Panel 2')

##########################
# 백테스트 결과 
##########################
os.chdir(path + '/Results')
result = pd.read_excel('res_201973.xlsx')

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

y1 = (result['longShort_return'].fillna(0)+1).cumprod() * 100
y2 = (result['I.101_return'].fillna(0) + 1).cumprod() * 100
y3 = (result['long'].pct_change().fillna(0) + 1).cumprod() * 100
y4 = y1 - y2
y5 = y4 - y2

df = pd.DataFrame({'port' : y1,
                   'bm' : y2,
                   'longOnly' : y3,
                   'ER_port' : y4,
                   'ER_long' : y5})
df['dd'] = drawdown(df['port']).values
df['zeros']= np.zeros(len(df))
df.reset_index(inplace=True)
df.columns = ['x', 'port', 'bm', 'longOnly', 'ER_port', 'ER_long', 'dd', 'zeros']
source = ColumnDataSource(df)

p1 = figure(plot_width=1200, plot_height=400,
           x_axis_type="datetime", x_axis_label='Date', y_axis_label='Relative Return')

p1.title.text = "Adaptive Multi Factor Allocatioin using ESI Index (excluding Size factor)"
p1.title.align = "center"

p1.extra_y_ranges = {"foo": Range1d(start=y4.min(), end = y4.max())}
p1.add_layout(LinearAxis(y_range_name = "foo", axis_label = 'Excess Return'), 'right')

p1.line('x', 'port', source = source, legend="Portfolio", color = 'red', line_width=2)
p1.line('x', 'bm', source = source, legend="KOSPI200", color = 'grey', line_width=2)

p1.varea('x', y1='zeros', y2='ER_port', source = source, legend="Excess Return", color = 'green', alpha =0.2, y_range_name="foo")

p1.legend.location = "top_left"
p1.legend.click_policy="mute"
p1.add_tools(HoverTool(tooltips=[('Date','@x{%Y-%m-%d}'), ('Port','@port'), ('BM', '@bm'), ('Excess', '@ER_port')], 
                                 formatters={'x' : 'datetime'}, 
                                 mode = 'vline',
                                 show_arrow=False, point_policy='follow_mouse'))


p2 = figure(plot_width=1200, plot_height=200, x_range = p1.x_range, x_axis_type="datetime")
p2.varea(x='x', y1 = 'zeros', y2 = 'dd', source = source, legend="Drawdown")
p2.legend.location = "bottom_left"

date_slider = DateRangeSlider(title="Date Range: ", 
                              start=result.index[0],
                              end=result.index[-1], 
                              value=(result.index[0], result.index[-1]), 
                              step=1)


def update_data(attrname, old, new):
    # Get the current slider values
    x_start = datetime.fromtimestamp(date_slider.value[0]/1000)
    x_end = datetime.fromtimestamp(date_slider.value[1]/1000)   
    x_start = pd.to_datetime(x_start)
    x_end = pd.to_datetime(x_end)    
    #print(x_start)
    #print(x_end)
    # Generate new data
    new_df = df[(df['x']>= x_start) & (df['x'] <= x_end)]
    
    new_df.loc[:,'port'] = (new_df['port'].pct_change().fillna(0) + 1).cumprod() * 100
    new_df.loc[:,'bm'] = (new_df['bm'].pct_change().fillna(0) + 1).cumprod() * 100   
    new_df.loc[:,'longOnly'] = (new_df['longOnly'].pct_change().fillna(0) + 1).cumprod() * 100  
    new_df.loc[:,'ER_port'] = new_df['port'] - new_df['bm']
    new_df.loc[:,'ER_long'] = new_df['port'] - new_df['longOnly']
    new_df.loc[:,'dd'] = drawdown(new_df['port'].values).values 
    new_df = new_df.reset_index().iloc[:,1:]
    newdata = ColumnDataSource(new_df)    
    source.data = newdata.data

date_slider.on_change('value', update_data)
plots = column(p1, p2, date_slider)
panel_1 = Panel(child = plots, title='Panel 1')
tabs = Tabs(tabs = [panel_1, panel_2])
curdoc().add_root(tabs)
curdoc().title = "DateRangeSlider Example"
