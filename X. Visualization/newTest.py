# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 09:46:07 2019

@author: Woojin Ji
"""
from bokeh.plotting import figure, output_file, show
from bokeh.layouts import column
from bokeh.models import LinearAxis, Range1d
from bokeh.models import HoverTool
from bokeh.plotting import ColumnDataSource, figure
from bokeh.models.widgets import Tabs, Panel, DateRangeSlider
import numpy as np
import pandas as pd
import os
from bokeh.transform import factor_cmap
from bokeh.palettes import Spectral10
from datetime import datetime
from bokeh.io import curdoc

os.chdir('C:/Woojin/###. Git/Project_Q/X. Visualization')
#os.chdir('/Users/Woojin/Documents/GitHub/Project_Q/X. Visualization')         

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


# create a new plot with a title and axis labels
ts = pd.read_excel('test_timeseries.xlsx', sheet_name = 'Sheet1')

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

y1 = ( ts['long'].pct_change().fillna(0) + 1 ).cumprod() * 100
y2 = ( ts['I.101'].pct_change().fillna(0) + 1 ).cumprod() * 100

df = pd.DataFrame({'y1': y1, 'y2':y2 })
df['dd'] = drawdown(df['y1']).values
df['zeros']= np.zeros(len(df))
df.reset_index(inplace=True)
df.columns = ['x', 'y1', 'y2', 'dd', 'zeros']

source = ColumnDataSource(df)
p1 = figure(plot_width=1200, plot_height=400, title="simple line example",
           x_axis_type="datetime", x_axis_label='Date', y_axis_label='Price')
p1.line('x', 'y1', source = source, legend="Portfolio", color = 'red', line_width=2)
p1.line('x', 'y2', source = source, legend="KOSPI200", color = 'grey', line_width=2)
p1.legend.location = "top_left"
p1.legend.click_policy="hide"
p1.add_tools(HoverTool(tooltips=[('Date','@x{%Y-%m-%d}'), ('Port','@y1'), ('BM', '@y2')], 
                                 formatters={'x' : 'datetime'}, 
                                 mode = 'vline',
                                 show_arrow=False, point_policy='follow_mouse'))


p2 = figure(plot_width=1200, plot_height=200, x_range = p1.x_range, x_axis_type="datetime")
p2.varea(x='x', y1 = 'zeros', y2 = 'dd', source = source)

date_slider = DateRangeSlider(title="Date Range: ", 
                              start=ts.index[0],
                              end=ts.index[-1], 
                              value=(ts.index[0], ts.index[-1]), 
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
    new_df.loc[:,'y1'] = (new_df['y1'].pct_change().fillna(0) + 1).cumprod() * 100
    new_df.loc[:,'y2'] = (new_df['y2'].pct_change().fillna(0) + 1).cumprod() * 100   
    new_df.loc[:,'dd'] = drawdown(new_df['y1'].values).values 
    new_df = new_df.reset_index().iloc[:,1:]
    newdata = ColumnDataSource(new_df)    
    source.data = newdata.data
   
date_slider.on_change('value', update_data)


plots = column(p1, p2, date_slider)
panel_1 = Panel(child = plots, title='Panel 1')
tabs = Tabs(tabs = [panel_1, panel_2])
curdoc().add_root(tabs)
curdoc().title = "DateRangeSlider Example"
