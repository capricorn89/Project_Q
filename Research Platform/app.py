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
import sys
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

import dataload
nameData, econ_data, index_data, econ_ticker, index_ticker = dataload.get_data()

app = dash.Dash()
server = app.server

df = pd.concat([index_data['CNY Curncy'],index_data['DXY Curncy']], axis=1)

df = pd.concat(index_data, axis=1)

# make dropdown option
index_opts = []
for c in df.columns:
    label = nameData.loc[c].values[0]
    option_i = {'label':label,'value':c}
    index_opts.append(option_i)


##############################################################################
# Display Layout
##############################################################################
    
app.layout = html.Div([
    # Setting the main title of the Dashboard
    html.H1("Data Analysis Testing", style={"textAlign": "center"}),
    
    # Dividing the dashboard in tabs
    dcc.Tabs(id="tabs", children=[
            
        # Defining the layout of the first Tab
        dcc.Tab(label='Indices', children=[
            html.Div([
                html.H1("Stock and Currency Indices", 
                        style={'textAlign': 'center'}),
                        
                # Adding the first dropdown menu 
                dcc.Dropdown(id='my-dropdown',
#                             options=[{'label': 'DXY Curncy', 'value': 'DXY Curncy'},
#                                      {'label': 'CNY Curncy','value': 'CNY Curncy'}], 
                             options=index_opts,
                             value='DXY Curncy',
                             style={"display": "block", "margin-left": "auto", 
                                    "margin-right": "auto", "width": "60%"}),

                # and the subsequent time-series graph
                dcc.Graph(id='curncy')
                
                
            ], className="container"),
        ]),

        # Defining the layout of the second tab
        dcc.Tab(label='nothing', children=[
            html.H1("nothing", 
                    style={"textAlign": "center"})
        ])
    ])
])


##############################################################################
# update graph
##############################################################################

@app.callback(Output('curncy', 'figure'),
              [Input('my-dropdown', 'value')])

def update_graph(stock):
    
    df_new = df.loc[:, stock].dropna()
    col = ['Date']+[df_new.name]
    df_new = df_new.reset_index()
    df_new.columns = col
    
    trace = []
    print(stock)
    trace.append(dict(
            x=df_new['Date'],
            y=df_new[stock],
            mode='lines', opacity=0.7))


    figure = {'data': trace,
              'layout': go.Layout(colorway=["#5E0DAC", '#FF4F00', '#375CB1', 
                                            '#FF7400', '#FFF400', '#FF0056'],
            height=600,
            title=f"Prices for {str(stock)} Over Time",
            xaxis={"title":"Date",
                   'rangeselector': {'buttons': list([{'count': 1, 'label': '1M', 
                                                       'step': 'month', 
                                                       'stepmode': 'backward'},
                                                      {'count': 6, 'label': '6M', 
                                                       'step': 'month', 
                                                       'stepmode': 'backward'},
                                                      {'step': 'all'}])},
                   'rangeslider': {'visible': True}, 'type': 'date'},
             yaxis={"title":"Price (USD)"})}
    
    figure = go.FigureWidget(figure)
    
    def zoom(layout, xrange):
        in_view = df_new.loc[figure.layout.xaxis.range[0]:figure.layout.xaxis.range[1]]
        figure.layout.yaxis.range = [in_view.High.min() , in_view.High.max() ]

    figure.layout.on_change(zoom, 'xaxis.range')
    
    return figure
            
if __name__ == '__main__':
    app.run_server(debug=True)