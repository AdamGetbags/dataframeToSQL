# -*- coding: utf-8 -*-
"""
Create SQL database from dataframe in Python with SQL Alchemy 
https://docs.sqlalchemy.org/en/14/
@author: adam getbags
"""

# pip install SQLAlchemy
# pip install sqlite3 # should already be installed with python

#import modules
import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
# import sqlite3

# check SQL Alchemy version
print(sqlalchemy.__version__)
print(os.getcwd())

# # change directory if needed
# os.chdir('C:\\Users\\Username\\Folder')

symbols = ['GOOG', 'GOOG', 'AAPL', 'AAPL', 'AMZN', 'AMZN']
dates = [
    '12-13-2022', '12-14-2022', 
    '12-13-2022', '12-14-2022', 
    '12-13-2022', '12-14-2022'
]
prices = [100, 101, 102, 103, 104, 105]

data = pd.DataFrame(list(zip(symbols, dates, prices)),
    columns =['symbol', 'date', 'price'])

# create sql alchemy engine object
engine = create_engine("sqlite+pysqlite:///testApp.db", echo=True, future=True)

# create table
data.to_sql('dataTable', engine)

'''
engine.connect() has auto ROLLBACK
engine.begin() has auto COMMIT
''' 

# get a connection / get column names // auto ROLLBACK
with engine.connect() as conn:
    result = conn.execute(text("PRAGMA table_info(dataTable)"))
    for row in result:
        print(row)

# get a connection / fetch rows // auto ROLLBACK
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM dataTable"))
    for row in result:
        print(f"""
              index: {row.index}
              symbol: {row.symbol} 
              date: {row.date} 
              price: {row.price}
              """)
              
# # get a connection / drop a table // autocommit
with engine.begin() as conn:
    conn.execute(
        text("DROP TABLE dataTable"))
                 
# # close all checked in database connections 
# engine.dispose() 
