#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 13 22:54:06 2019

@author: Woojin
"""

import sqlite3
from sqlite3 import Error

path_mac = '/Users/Woojin/Documents/Github/Project_Q/'
path_win = 'C:/Woojin/###. Git/Project_Q/Value and Earnings Momentum'
# 1. Create Database
def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        conn.close()
 
                  
if __name__ == '__main__':
    create_connection('C:/Woojin/###. Git/Project_Q/Value and Earnings Momentum/rdata.db')