# -*- coding: utf-8 -*-
"""
Created on Fri Aug  9 08:27:58 2019

@author: Woojin
"""

import time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import pandas as pd
import re
import os
import pandas as pd
import PyPDF2
import pickle

from urllib.request import urlopen
import webbrowser


auth_key = 'e9066f944d3f098fb0cd3a07b53a2503f53be44d'
company_code = '042500'
start_date='20190807'

url = "http://dart.fss.or.kr/api/search.xml?auth="+auth_key+"&crp_cd="+company_code+"&start_dt="+start_date+"&bsn_tp=A001&bsn_tp=A002&bsn_tp=A003"

#STEP 4
resultXML=urlopen(url)  #this is for response of XML
result=resultXML.read() #Using read method

#STEP 5
xmlsoup=BeautifulSoup(result,'html.parser')

#STEP 6
data = pd.DataFrame()



