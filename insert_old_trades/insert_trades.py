# -*- coding: utf-8 -*-
"""
Created on Fri Mar 17 09:20:08 2023

@author: niraj.munot
"""

import re 

test_str = "NIFTY22"
res = re.findall('([a-zA-Z ]*)\d*.*', test_str)
print(res[0])


import mysql.connector
import datetime
import pandas as pd

mydb = mysql.connector.connect(
  host="192.168.3.86",
  user="root",
  password="12345678",
   database = "strategies"
)
mycursor = mydb.cursor()

a = "SELECT * FROM trades"
mycursor.execute(a)
a = mycursor.fetchall()
mycursor.close()
mydb.close()

columns = ["id", "strategyname", "entrytime", "exittime", "symbol", "entryprice", "entryprice_executed", "exitprice", "exitprice_executed", "positiontype",
           "quantity", "token", "date", "exit_reason", "forward_test"]
a = [{val : i[columns.index(val)] for val in columns} for i in a]

a = pd.DataFrame(a)
a['lotsize'] = a.apply(lambda row : 50 if re.findall('([a-zA-Z ]*)\d*.*', row['symbol'])[0] == "NIFTY" else 25, axis = 1)

from sql_manager.sql_control import Manage_strategies

sq = Manage_strategies()

for i in range(len(a)):
    c = a.iloc[i]
    sq.add_trade(c['strategyname'], c['entrytime'], c['symbol'], c['entryprice'], c['entryprice_executed'], c['positiontype'], int(c['quantity']), int(c['token']), 
                 c['exittime'], c['exitprice'], c['exitprice_executed'], "", c['date'], forward_test = c['forward_test'], lot_size = int(c['lotsize']))
    
    
