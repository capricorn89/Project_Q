#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep  2 23:27:23 2019

@author: Woojin
"""


import pandas as pd
import numpy as np
import os, inspect
from scipy import stats
import statsmodels.tsa.stattools as ts
import time

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(path)

# 1) 시계열 데이터 업로드

# 지금은 일단 전체 시계열로 페어를 만들고 찾지만, 
# 나중에는 유의미한 페어는 따로 데이터베이스에 저장했다가 일단 걔들중에 업데이트 된거만 확인
# 그리고 시간이 남으면 전체 페어에 대해서 새롭게 유의미하게 페어가 된 조합을 찾아야?

