# -*- coding: utf-8 -*-
"""
Created on Wed Jun 19 07:29:57 2019

@author: Woojin
"""

import pandas as pd
import os
import numpy as np
import datetime
import pymysql
from tqdm import tqdm
import calendar
from scipy import stats

os.chdir('C:/Woojin/###. Git/Project_Q/II. Adaptive Multi Factor Strategy')

import backtest_pipeline_ver2 as bt
import util


from bokeh.plotting import figure, output_file, show
from bokeh.layouts import column
from bokeh.models import LinearAxis, Range1d
from bokeh.transform import factor_cmap
from bokeh.palettes import Spectral10
from bokeh.models import ColumnDataSource
from bokeh.models import HoverTool

result = pd.read_excel('res_2019618.xlsx')

x = result.index
y1 = (result['longShort_return'].fillna(0)+1).cumprod() * 100
y2 = (result['I.101_return'].fillna(0) + 1).cumprod() * 100
y4 = (result['long'].pct_change().fillna(0) + 1).cumprod() * 100
y3 = y1 - y2
y5 = y4 - y2

p1 = figure(plot_width=800, plot_height=300, 
           x_axis_type="datetime", x_axis_label='Date', y_axis_label='Relative Return')
p1.title.text = "Adaptive Multi Factor Allocatioin using ESI Index (excluding Size factor)"
p1.title.align = "center"
p1.extra_y_ranges = {"foo": Range1d(start=y3.min(), end = y3.max())}
p1.add_layout(LinearAxis(y_range_name = "foo", axis_label = 'Excess Return'), 'right')

p1.line(x, y1, legend="Portfolio", color = 'red', line_width=2)
p1.line(x, y2, legend="KOSPI200", color = 'grey', line_width=2)
p1.line(x, y4, legend="LongOnly", color = 'purple', line_width=2)
p1.varea(x, y1=np.zeros(len(y3)), y2=y3, legend="Excess Return", color = 'green', alpha =0.2, y_range_name="foo")
p1.varea(x, y1=np.zeros(len(y5)), y2=y5, legend="Excess Return (LongOnly)", color='yellow', alpha = 0.4, y_range_name="foo")
p1.legend.location = "top_left"
p1.legend.click_policy="hide"

p2 = figure(plot_width=800, plot_height=100, x_range = p1.x_range, x_axis_type="datetime")
p2.varea(x=x, y1 = np.zeros(len(y1)), y2 = util.get_drawdown(y1), legend="Drawdown")

p2.legend.location = "bottom_left"

show(column(p1,p2))


#hover = p1.select(dict(type=HoverTool))
#hover.tooltips= [("Date", "$x"),
#                ("Portfolio : ", "$y1"),
#                ("KOSPI200 : ", "$y2"), 
#                ("Excess Return : ", "$y3"),
#                ("Portfolio (Long Only) : ", "$y4"),
#                ("Excess Return (Long Only) : ", "$y5")]
#hover.mode = 'mouse' 
##    formatters = {
##                 "Date" : 'datetime',
##                 'y1' : 'printf',
##                 'y2' : 'printf',
##                 'y3' : 'printf',
##                 'y4' : 'printf',
##                 'y5' : 'printf',
##                 },
#show(p1)

