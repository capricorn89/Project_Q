# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 08:56:50 2019

@author: USER
"""

'''
< 부품소재 강소기업 바스켓 전략 >

목표 : 재무적으로 안정적인 부품,소재 업종 강소기업에 투자하는 ETF

유니버스 :
    - WICS 기준 코스피, 코스닥의 소재, IT 섹터에 속한 종목 중 시가총액 상위 100개 제외
    - 상장 3개월 미만 제외
    - 관리종목, 투자주의환기종목, 거래정지종목 제외
    - 시총 500억 미만 제외
    - 60일 평균 거래대금 5억 미만 제외
    - 최근 2년 연속 적자기업 제외

종목 선정 기준 : Piotroski F-score + MSCI Quality Index Methodology
    - 직전 4분기 당기순이익 합 > 0
    - 직전 4분기 영업활동현금흐름 합 > 0
    - 직전 4분기 영업활동현금흐름 합 > 직전 4분기 당기순이익 합


분기 말 리밸런싱

'''

import os
import pandas as pd
import numpy as np
import datetime
path = 'D:/Woojin/GitHub/Project_Q/V. Small Cap strategy'
os.chdir(path)
import util as ut

rdata = pd.ExcelFile(path + '/Data/data.xlsx')
