# -*- coding: utf-8 -*-
"""
Created on Wed May 15 10:30:17 2019

@author: Woojin
"""

import sqlite3
from sqlite3 import Error
import os

path_mac = '/Users/Woojin/Documents/Github/Project_Q/'
path_win = 'C:/Woojin/###. Git/Project_Q/I. Value and Earnings Momentum'
os.chdir(path_win)

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


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def main():
    create_connection('C:/Woojin/###. Git/Project_Q/I. Value and Earnings Momentum/rdata.db')
                      
    database = "rdata.db"
 
    sql_create_table_firmName = """ CREATE TABLE IF NOT EXISTS firmName (
                                        id text PRIMARY KEY,
                                        name text NOT NULL
                                    ); """
 
    sql_create_table_firmInfo = """ CREATE TABLE IF NOT EXISTS firmInfo (
                                        id text,
                                        date text NOT NULL,
                                        market text,
                                        sector text,
                                        in_k200 integer,
                                        in_kq150 integer,
                                        risk_1 integer,
                                        risk_2 integer,
                                        PRIMARY KEY (id, date)
                                    ); """
 
    sql_create_table_price = """ CREATE TABLE IF NOT EXISTS price (
                                    id text,
                                    date text NOT NULL,
                                    price REAL NOT NULL,
                                    PRIMARY KEY (id, date)
                                ); """

    
    sql_create_table_quantData_Q = """ CREATE TABLE IF NOT EXISTS quantData_Q (
                                    id text,
                                    date text NOT NULL,
                                    ocf REAL,
                                    ocf_TTM REAL,
                                    cf_TTM REAL,
                                    opm_TTM REAL,
                                    PRIMARY KEY (id, date)
                                ); """
    
    
    sql_create_table_quantData_M = """ CREATE TABLE IF NOT EXISTS quantData_M (
                                    id text,
                                    date text NOT NULL,
                                    vol_20MA REAL,
                                    netbuy_20 REAL,
                                    PRIMARY KEY (id, date)
                                ); """
    
    
    def create_connection_db(db_file):
        """ create a database connection to the SQLite database
            specified by db_file
        :param db_file: database file
        :return: Connection object or None
        """
        try:
            conn = sqlite3.connect(db_file)
    
            return conn
        except Error as e:
            print(e)
     
        return None
    
    # create a database connection
    conn = create_connection_db(database)
    if conn is not None:
        create_table(conn, sql_create_table_firmName)
        create_table(conn, sql_create_table_firmInfo)
        create_table(conn, sql_create_table_price)
        create_table(conn, sql_create_table_quantData_Q)
        create_table(conn, sql_create_table_quantData_M)
    else:
        print("Error! cannot create the database connection.")        
        
        
if __name__ == '__main__':
    main()


