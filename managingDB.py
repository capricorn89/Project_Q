#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 13 22:54:06 2019

@author: Woojin
"""

import sqlite3
from sqlite3 import Error

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
 
create_connection('/Users/Woojin/Documents/Github/Project_Q/Project_Q.db')