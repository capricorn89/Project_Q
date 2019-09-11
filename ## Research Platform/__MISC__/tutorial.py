# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 15:25:52 2019

@author: Woojin
"""


import eikon as ek
import pandas as pd

ek.set_app_key('e14c5b0ed4a6437baba5b2d6b644d9a3bd59457a')


# 1) News

test = ek.get_news_headlines('R:PRESS/FT', date_from='2018-06-29T09:00:00', date_to='2018-07-03T18:00:00')
headlines = ek.get_news_headlines('EU AND POL',1)
story = headlines.iat[0,2]
ek.get_news_story(story)


# 2) Time-series data

ek.get_timeseries(['GLDW.K','GLD'], fields = ["Close"] ,
                       start_date="2018-06-01",  
                       end_date="2018-07-05")

ek.get_timeseries('069500.KS', fields = ['TR.TtlCmnSharesOut'], start_date="2018-06-01", end_date="2018-07-13")

ek.get_timeseries('GBGV5YUSAC=R', fields = ['CF_LAST'], start_date="2018-06-01", end_date="2018-07-13")


# 3) Static data

df = ek.get_data('226490.KS',fields = ['TR.FundLaunchDate'])

df = ek.get_data('069500.KS', fields = ['TR.TtlCmnSharesOut'])



### For more details

# https://developers.thomsonreuters.com/eikon-apis/eikon-data-apis/quick-start    --> Quick start
# https://developers.thomsonreuters.com/eikon-apis/eikon-data-apis/learning       --> Tutorial

