# -*- coding: utf-8 -*-
"""
Created on Fri Dec 13 15:01:38 2019

@author: Woojin
"""
import os, inspect
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(path + '/img/')

def dualAxesPlot(dualData, fileName, names = None):
    if names == None:
        names = dualData.columns.values
    else:
        names = names
    fig = make_subplots(specs = [[{"secondary_y": True}]]) # Create figure with secondary y-axis
    # Add traces
    fig.add_trace(
        go.Scatter(x=dualData.index, y=dualData.iloc[:,0].values, name=names[0]+"(L)",
                   connectgaps = True),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=dualData.index, y=dualData.iloc[:,1].values, name=names[1]+"(R)",
                   connectgaps = True),
        secondary_y=True
    )
    # Add figure title
    fig.update_layout(
        title_text= names[0] + " vs. " + names[1],
        xaxis_tickformat = '%Y-%m-%d'
    )
    # Set x-axis title
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(showgrid = False)
    # Set y-axes titles
    fig.update_yaxes(title_text="<b>" + dualData.columns.values[0] + "</b> ", secondary_y=False)
    fig.update_yaxes(title_text="<b>" + dualData.columns.values[1] + "</b>", secondary_y=True)
    fig.write_html(fileName + '.html', auto_open=True)

def singleAxesPlot(data, fileName, names = None):
    if names == None:
        names = data.columns.values
    else:
        names = names
    fig = go.Figure()
    # Add traces
    for i in range(len(names)):
        fig.add_trace(
                go.Scatter(x = data.index, y = data.iloc[:,i].values, name = names[i]))
    # Add figure title
    fig.update_layout(
        title_text= fileName,
        xaxis_tickformat = '%Y-%m-%d'
    )
    fig.write_html(fileName + '.html', auto_open=True)
