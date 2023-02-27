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
    qty = strategydata['qty'] if not data['qty'] else data['qty']
    splt = strategydata['freezequantity'] if not data['split_qty'] else data['split_qty']
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
        "reason" : "" if not data['reason'] else data['reason']
        }
    print(orderdict)
    
    t1 = threading.Thread(target = lambda : Ex.singleorder(orderdict)).start()
    return {"error" : False, "data" : [refno], "status" : "Order placed successfully."}

    # try: 
    #     data = request.json
    #     strategy = data['strategyname']
    #     # logger.debug(f"Received API call for Order Placement in {strategy}.")
    #     if strategy in sqoff : 
    #         return {"error" : True, "data" : [], "status" : "Strategy is already squared off."}
    #     if strategy in cx.strategies : 
    #         stdetails = cx.strategies_config[strategy]
    #         is_forward = True if stdetails['execution_type'] == "Forward" else False 
    #         t1 = threading.Thread(target = lambda : om.place_order(strategy, data['token'], data['transactiontype'], stdetails['qty'], data['price'], stdetails['product_type'], is_forward = is_forward, reason = data['reason'])).start()
    #         return {"error" : False, "data" : [{'filled_at' :om.get_ltp(data['token']) }], "status" : "Order executed successfully."}
    #     else : 
    #         logger.debug("Error  : Strategy name is not defined.")
    #         return {"error" : True, "data": [], "status": "Strategy name not defined."}  
    #     return data
    # except Exception as e :
    #     print(e)
    #     return {"error":True, "data" : [], "status" : str(e)} 
    
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)
