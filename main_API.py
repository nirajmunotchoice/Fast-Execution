# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 11:30:32 2023

@author: niraj.munot
"""

from flask import Flask, request, session, redirect, url_for, render_template
import datetime
import requests
import threading 
import time
import pandas as pd
from waitress import serve
import logging 
import redis
from sql_manager.sql_control import Manage_strategies
from Execution.execute import Execution
from TS_counter.redis_counter import redis_ts
sq = Manage_strategies()
Ex = Execution()
rts = redis_ts()
app = Flask(__name__)
turnoff = []

@app.route('/placeorder', methods=['POST'])
def placeorder():
    global turnoff 
    data = request.json
    
    if data['strategyname'] in turnoff :
        return {"error" : True, "data" : [], "status" : "Strategy is already turned off."}
    
    strategydata = sq.viewone(data['strategyname'])
    if strategydata == [] : 
        return {"error" : True, "data" : [], "status" : "Wrong strategy name"}
    
    strategydata = strategydata[0]
    qty = strategydata['qty'] if data.get("qty") == None or data.get("qty") == False else data['qty']
    splt = strategydata['freezequantity'] if data.get('split_qty') == None or data.get('split_qty') == False else data['split_qty']
    refno = rts.incr()
    orderdict = {
        "reftag" : refno,
        "strategyname" : data['strategyname'], 
        "token" : int(data['token']), 
        "transactiontype" : data['transactiontype'],
        "qty" : int(qty), 
        "split_qty" : int(splt), 
        "to_split" : False if splt > qty else True, 
        "price" : Ex.get_ltp(data['token'])['ltp'] if not data['price'] else data['price'],
        "is_forward" : True if strategydata['execution_type'] == "Forward" else False, 
        "exchange" : strategydata['exchange'], 
        "reason" : "" if data.get('reason') == None or data.get('reason') == False else data['reason']
        }
    print(orderdict)
    
    t1 = threading.Thread(target = lambda : Ex.singleorder(orderdict)).start()
    return {"error" : False, "data" : [refno], "status" : "Order placed successfully."}

@app.route("/addstrategy", methods = ['POST'])
def addstrategy(): 
    try: 
        data = request.json
        strategy = data['strategyname']
        d = sq.viewone(strategy)
        if d != [] : 
            raise Exception("Strategy Already Exists")

        # sq.add_strategy(strategy, exectype, quantity, product_type) 
        sq.add_strategy(strategy, data['exectype'], data['quantity'], data['product_type'], freezequantity = data.get('freezequantity'), 
                        symbol = data.get('symbol'), grouptag = data.get('grouptag'), exchange = data.get('exchange'))
        
        return {"error":False, "data" : [], "status" : "Strategy Added"} 
    except Exception as e:
        return {"error":True, "data" : [], "status" : str(e)} 

@app.route("/updatestrategy", methods = ['POST'])
def updatestrategy(): 
    try:  
        data = request.json
        strategy = data['strategyname']
        d = sq.viewone(strategy)
        if d == [] :
            raise Exception( "Strategy doesnt exist")
        
        sq.update_strategy(strategy, exectype = data.get('exectype'), quantity = data.get('quantity'), product_type = data.get('product_type'), freezequantity = data.get('freezequantity'), 
                           symbol = data.get('symbol'), grouptag = data.get('grouptag'), exchange = data.get('exchange'))
        
        return {"error":False, "data" : [], "status" : "Strategy Updated"} 
    except Exception as e: 
        return {"error":True, "data" : [], "status" : str(e)} 
    
    
@app.route("/getstrategy", methods = ['GET'])
def get_strategies():
    try: 
        stname = request.json['strategyname']
        data = sq.viewone(stname)
        if data == [] :
            raise Exception( "Strategy doesnt exist")
        return {"error":False, "data" : data, "status" : "Data Received"} 
    except Exception as e : 
        return {"error":True, "data" : [], "status" : str(e)} 

@app.route("/allstrategies", methods = ['GET'])
def allstrategies():
    try: 
        return {"error":False, "data" : sq.viewall(), "status" : "Data Received"} 
    except Exception as e : 
        return {"error":True, "data" : [], "status" : str(e)} 

@app.route("/api/lastupdate/<token>")
def get_last_data(token):
    try: 
        a = Ex.get_ltp(token)
        return {"Success" : True, "Data" : a, "Error": False}
    except Exception as e : 
        return {"Success" : False, "Error" : str(e)}

@app.route("/api/orderbook")
def get_orderbook(): 
    try : 
        return {"Success" : True, "Data" : Ex.get_orderbook(), "Error" : False}
    except  Exception as e : 
        return {"Success" : False, "Data" : [], "Error" : str(e)}

@app.route("/api/tradebook")
def get_tradebook(): 
    try : 
        return {"Success" : True, "Data" : Ex.get_tradebook(), "Error" : False}
    except  Exception as e : 
        return {"Success" : False, "Data" : [], "Error" : str(e)}

@app.route("/api/netposition")
def get_netpositions(): 
    try : 
        return {"Success" : True, "Data" : Ex.get_netpositions(), "Error" : False}
    except  Exception as e : 
        return {"Success" : False, "Data" : [], "Error" : str(e)}
    
def stop_trade_recon(self):
    Ex.trades_process = False

def start_trade_recon(self):
    Ex.trades_process = True
    t1 = threading.Thread(target = Ex.trade_reconcile).start()
    
def active_order_threads(self):
    return Ex.recon_threads

def stop_recon_threads(self, reftag):
    Ex.recon_threads[reftag]['is_running'] = False

    
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)



