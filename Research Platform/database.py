# -*- coding: utf-8 -*-
"""
Created on Fri Dec 13 15:01:38 2019

@author: Woojin
"""


import os, inspect
import pandas as pd
import numpy as np
import matplotlib
from matplotlib import font_manager, rc
import matplotlib.pyplot as plt
import xlsxwriter

font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/malgun.ttf").get_name()
rc('font', family=font_name, size =8)
matplotlib.rcParams['axes.unicode_minus'] = False  

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(path)

df_excel = pd.ExcelFile('ResearchData.xlsx')
