# -*- coding: utf-8 -*-
"""
Created on Wed Jul  3 17:17:04 2019

@author: user
"""
from bokeh.plotting import figure, output_file, show
from bokeh.layouts import column
from bokeh.models import LinearAxis, Range1d, HoverTool, LinearColorMapper, BasicTicker, PrintfTickFormatter, ColorBar
from bokeh.plotting import ColumnDataSource, figure
from bokeh.models.widgets import Tabs, Panel, DateRangeSlider
import numpy as np
import pandas as pd
import os
from bokeh.transform import factor_cmap
from bokeh.palettes import Spectral10
from datetime import datetime
from bokeh.io import curdoc
import statsmodels.api as sm

os.chdir('C:/Woojin/###. Git/Project_Q/X. Visualization')
#os.chdir('/Users/Woojin/Documents/GitHub/Project_Q/X. Visualization')      
# 히트맵 그림 그리기
from math import pi
price = pd.read_excel('test_timeseries.xlsx', sheet_name = 'price')
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

source = ColumnDataSource(corr)    
    
colors = ["#75968f", "#a5bab7", "#c9d9d3", "#e2e2e2", "#dfccce", "#ddb7b1", "#cc7878", "#933b41", "#550b1d"]
mapper = LinearColorMapper(palette=colors, low=corr['corrcoef'].min(), high=corr['corrcoef'].max())

TOOLS = "hover,save,pan,box_zoom,reset,wheel_zoom"

p4 = figure(title='Correlation Heatmap',
           x_range=names, y_range=list(reversed(names)),
           x_axis_location="above", plot_width=900, plot_height=400,
           tools=TOOLS, toolbar_location='below',
           tooltips=[('date', '@Month @Year'), ('rate', '@rate%')])

p4.grid.grid_line_color = None
p4.axis.axis_line_color = None
p4.axis.major_tick_line_color = None
p4.axis.major_label_text_font_size = "10pt"
p4.axis.major_label_standoff = 0
p4.xaxis.major_label_orientation = pi / 3

p4.rect(x="var_1", y="var_2", width=1, height=1,
        source=source,
        fill_color={'field': 'corrcoef', 'transform': mapper},
        line_color=None)

color_bar = ColorBar(color_mapper=mapper, major_label_text_font_size="5pt",
                     ticker=BasicTicker(desired_num_ticks=len(colors)),
                     formatter=PrintfTickFormatter(format="%d%%"),
                     label_standoff=6, border_line_color=None, location=(0, 0))

p4.add_layout(color_bar, 'right')

heat_slider = DateRangeSlider(title="Date Range: ", 
                              start=ret.index[0],
                              end=ret.index[-1], 
                              value=(ret.index[0], ret.index[-1]), 
                              step=1)

def update_heatmap(attrname, old, new):
    # Get the current slider values
    x_start = datetime.fromtimestamp(heat_slider.value[0]/1000)
    x_end = datetime.fromtimestamp(heat_slider.value[1]/1000)   
    x_start = pd.to_datetime(x_start)
    x_end = pd.to_datetime(x_end)    
    #print(x_start)
    #print(x_end)
    # Generate new data
    new_df = ret.loc[x_start:x_end,:].corr()
    new_df = new_df.where(np.triu(np.ones(new_df.shape)).astype(np.bool))
    new_df = new_df.stack().reset_index()
    new_df.columns = ['var_1', 'var_2', 'corrcoef']

    newdata = ColumnDataSource(new_df)    
    source.data = newdata.data

heat_slider.on_change('value', update_heatmap)
heatmap = column(p4, heat_slider)
panel_3 = Panel(child = heatmap, title='Panel 3')

tabs = Tabs(tabs = [panel_3])
curdoc().add_root(tabs)
curdoc().title = "DateRangeSlider Example"
