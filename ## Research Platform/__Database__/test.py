# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 15:25:52 2019

@author: Woojin
"""

import eikon as ek
import pandas as pd
import numpy as np
import os, inspect
import sqlite3

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(path)


import get_data as gd

df = gd.fromDB('DB_ver_1.1.db')
df.econ('USFEFRH', 20190801, 20190930)

