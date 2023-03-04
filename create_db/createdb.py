# -*- coding: utf-8 -*-
"""
Created on Tue Feb 14 10:28:06 2023

@author: niraj.munot
"""


import mysql.connector
import datetime
import pandas as pd

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Choice@123",
   database = "test_struct"
)

mycursor = mydb.cursor()

mycursor.execute("""CREATE TABLE positions1
                 (
                  refno INT NOT NULL,   
                  strategyname VARCHAR(255), 
                  tm DATETIME NOT NULL, 
                  symbol VARCHAR(255), 
                  price FLOAT(10),
                  traded_price FLOAT(10),
                  positiontype VARCHAR(255),
                  qty INT,
                  traded_qty INT,
                  token INT,
                  orderstatus VARCHAR(255),
                  is_exec boolean,
                  is_recon boolean,
                  is_sqoff boolean,
                  is_forward boolean,
                  sent_orders INT, 
                  exec_orders INT,
                  PRIMARY KEY (refno))""")

vals = ["refno", "strategyname", "tm", "symbol", "price", "traded_price", "positiontype", "qty", "traded_qty", "token", "orderstatus", "is_exec", "is_recon",
        "is_sqoff", "is_forward", "sent_orders", "exec_orders"]

mycursor.execute("""
               CREATE TABLE reforderid
               (
                   id INT NOT NULL AUTO_INCREMENT,  
                   refno INT NOT NULL, 
                   qty INT NOT NULL,
                   orderid INT NOT NULL, 
                   orderstatus VARCHAR(255),
                   token INT NOT NULL,
                   placed_at DATETIME NOT NULL, 
                   PRIMARY KEY (id))
               """)
               
mycursor.execute("""
               CREATE TABLE refidorderbook1
               (
                   id INT NOT NULL AUTO_INCREMENT,  
                   refno INT NOT NULL,
                   strategyname VARCHAR(255) NOT NULL,
                   orderid INT NOT NULL, 
                   orderstatus VARCHAR(255),
                   token INT ,
                   transactiontype VARCHAR(255),
                   req_qty INT ,
                   exec_qty INT ,
                   symbol VARCHAR(255),
                   is_done boolean,
                   is_exec boolean,
                   placed_at DATETIME ,
                   recon_at DATETIME ,
                   order_desc VARCHAR(255),
                   is_traded boolean,
                   traded_qty INT,
                   requested_price FLOAT(10),
                   traded_price FLOAT(10),
                   traded_at DATETIME, 
                   PRIMARY KEY (id))
               """)


lst = ["refno", "strategyname", "orderid", "orderstatus", "token" , "transactiontype" , "req_qty" , "exec_qty" , "symbol" , "is_done" , "is_exec" , "placed_at", 
       "recon_at" , "order_desc", "is_traded", "requested_price", "traded_price", "traded_at"]


lsttrades = ['id', "strategyname", "entrytime", "exittime", "symbol", "entryprice", "entryprice_executed", "exitprice", "exitprice_executed", "positiontype", 
             "quantity", "token", "date", "exit_reason", "forward_test"]