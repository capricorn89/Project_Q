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

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

import dataload
nameData, econ_data, index_data, econ_ticker, index_ticker = dataload.get_data()
