# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 09:46:07 2019

@author: Woojin Ji
"""
from bokeh.plotting import figure, output_file, show
from bokeh.layouts import column
from bokeh.models import LinearAxis, Range1d, HoverTool, LinearColorMapper, BasicTicker, PrintfTickFormatter, ColorBar
from bokeh.plotting import ColumnDataSource, figure
from bokeh.models.widgets import Tabs, Panel, DateRangeSlider, PreText, Select
import numpy as np
import pandas as pd
import os
from bokeh.transform import factor_cmap
from bokeh.palettes import Spectral10
from datetime import datetime
from bokeh.io import curdoc
import statsmodels.api as sm

os.chdir('D:/Woojin/GitHub/Project_Q/X. Visualization')
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
panel_2 = Panel(child = p3, title = 'Economic Sentiment Indicator')

# create a new plot with a title and axis labels
ts = pd.read_excel('test_timeseries.xlsx', sheet_name = 'Sheet1', index_col = 0)

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
p1.line('x', 'y1', source = source, legend="Portfolio", color = 'red', alpha = 0.8,
        muted_color = 'red', muted_alpha = 0.2, line_width=2)
p1.line('x', 'y2', source = source, legend="KOSPI200", color = 'grey',alpha = 0.8,
        muted_color = 'grey', muted_alpha = 0.2, line_width=2)
p1.legend.location = "top_left"
p1.legend.click_policy="mute"
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
panel_1 = Panel(child = plots, title='Portfolio Performance')


# 히트맵 그림 그리기

try:
    from functools import lru_cache
except ImportError:
    # Python 2 does stdlib does not have lru_cache so let's just
    # create a dummy decorator to avoid crashing
    print ("WARNING: Cache for this example is available on Python 3 only.")
    def lru_cache():
        def dec(f):
            def _(*args, **kws):
                return f(*args, **kws)
            return _
        return dec
    
from math import pi
price = pd.read_excel('test_timeseries.xlsx', sheet_name = 'price', index_col = 0)
ret = price.pct_change()
corr = ret.corr()
corr = corr.where(np.triu(np.ones(corr.shape)).astype(np.bool))
corr = corr.stack().reset_index()
corr.columns = ['var_1', 'var_2', 'corrcoef']
names = []
for i in range(len(corr)):
    name = corr['var_1'][i]
    if np.isin(name, names):
        pass
    else:
        names.append(name)

heat_source = ColumnDataSource(corr)    
colors = ["#75968f", "#a5bab7", "#c9d9d3", "#e2e2e2", "#dfccce", "#ddb7b1", "#cc7878", "#933b41", "#550b1d"]
colors = list(reversed(colors))
mapper = LinearColorMapper(palette=colors, low=-1, high=1)
TOOLS = "hover,save,pan,box_zoom,reset,wheel_zoom"
p4 = figure(title='Correlation Heatmap',
           x_range=names, y_range=list(reversed(names)),
           x_axis_location="above", plot_width=900, plot_height=300,
           tools=TOOLS, toolbar_location='below',
           tooltips=[('Var_1', '@var_1'), ('Var_2', '@var_2'), ('Coef', '@corrcoef')])
p4.grid.grid_line_color = None
p4.axis.axis_line_color = None
p4.axis.major_tick_line_color = None
p4.axis.major_label_text_font_size = "10pt"
p4.axis.major_label_standoff = 0
#p4.xaxis.major_label_orientation = pi / 3
p4.rect(x="var_1", y="var_2", width=1, height=1,
        source=heat_source,
        fill_color={'field': 'corrcoef', 'transform': mapper},
        line_color=None)
color_bar = ColorBar(color_mapper=mapper, major_label_text_font_size="5pt",
                     ticker=BasicTicker(desired_num_ticks=len(colors)),
                     formatter=PrintfTickFormatter(format="%.1f"),
                     label_standoff=6, border_line_color=None, location=(0, 0))
p4.add_layout(color_bar, 'right')

def update_heatmap(attrname, old, new):
    # Get the current slider values
    x_start = datetime.fromtimestamp(heat_slider.value[0]/1000)
    x_end = datetime.fromtimestamp(heat_slider.value[1]/1000)   
    x_start = pd.to_datetime(x_start)
    x_end = pd.to_datetime(x_end)    
    #print(x_start)
    #print(x_end)
    # Generate new data
    new_ret = ret.loc[x_start:x_end,:]
    new_df = new_ret.corr()
    new_df = new_df.where(np.triu(np.ones(new_df.shape)).astype(np.bool))
    new_df = new_df.stack().reset_index()
    new_df.columns = ['var_1', 'var_2', 'corrcoef']   
    newdata = ColumnDataSource(new_df)    
    heat_source.data = newdata.data


def nix(val, lst):
    return [x for x in lst if x != val]

DEFAULT_TICKERS = price.columns.values
stats = PreText(text='', width=500)
ticker1 = Select(value=DEFAULT_TICKERS[0], options=nix(DEFAULT_TICKERS[1], DEFAULT_TICKERS))
ticker2 = Select(value=DEFAULT_TICKERS[1], options=nix(DEFAULT_TICKERS[0], DEFAULT_TICKERS))
source_static = ColumnDataSource(data=dict(date=[], t1=[], t2=[]))
tools = 'pan,wheel_zoom,xbox_select,reset'

ts1 = figure(plot_width=900, plot_height=200, tools=tools, x_axis_type='datetime')
#ts1.yaxis[0].formatter = PrintfTickFormatter(format = '0,0.0000')
ts1.line('date', 't1', source=source_static)

ts2 = figure(plot_width=900, plot_height=200, tools=tools, x_axis_type='datetime')
#ts2.yaxis[0].formatter = PrintfTickFormatter(format = '0,0.0000')
ts2.x_range = ts1.x_range
ts2.line('date', 't2', source=source_static)

def get_data(t1, t2):
    df1 = price.loc[:,t1]
    df2 = price.loc[:,t2]
    data = pd.concat([df1, df2], axis=1)
    data = data.dropna()
    data = data.reset_index()
    data.columns = ['date','t1','t2']
    return data

def ticker1_change(attrname, old, new):
    ticker2.options = nix(new, DEFAULT_TICKERS)
    update()

def ticker2_change(attrname, old, new):
    ticker1.options = nix(new, DEFAULT_TICKERS)
    update()

def update(selected=None):
    t1, t2 = ticker1.value, ticker2.value
    data = get_data(t1, t2)
    source_static.data = source.from_df(data[['date', 't1', 't2']])
    ts1.title.text, ts2.title.text = t1, t2


def update_ts(attrname, old, new):
    # Get the current slider values
    x_start = datetime.fromtimestamp(heat_slider.value[0]/1000)
    x_end = datetime.fromtimestamp(heat_slider.value[1]/1000)   
    x_start = pd.to_datetime(x_start)
    x_end = pd.to_datetime(x_end)    
    t1, t2 = ticker1.value, ticker2.value
    #print(x_start)
    #print(x_end)
    # Generate new data
    new_price = price.loc[x_start:x_end,[t1,t2]]
    new_price = new_price.reset_index()
    new_price.columns = ['date','t1','t2']   
    newPriceData = ColumnDataSource(new_price)    
    source_static.data = newPriceData.data

    
    
ticker1.on_change('value', ticker1_change)
ticker2.on_change('value', ticker2_change)

# set up layout

heat_slider = DateRangeSlider(title="Date Range: ", 
                              start=ret.index[0],
                              end=ret.index[-1], 
                              value=(ret.index[0], ret.index[-1]), 
                              step=1)

heat_slider.on_change('value', update_heatmap)
heat_slider.on_change('value', update_ts)

heatmap = column(ticker1, ticker2, heat_slider, p4, ts1, ts2)
panel_3 = Panel(child = heatmap, title='Macro at a Glance')
tabs = Tabs(tabs = [panel_1, panel_2, panel_3])

# initialize
update()

curdoc().add_root(tabs)
curdoc().title = "Plot Master"
