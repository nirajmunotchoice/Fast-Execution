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
from logging.config import dictConfig
from flask.logging import default_handler

class RequestFormatter(logging.Formatter):
    def format(self, record):
        record.url = request.url
        record.remote_addr = request.remote_addr
        record.jsonresp = [] if request.headers.get('Content-Type') == None else request.json
        return super().format(record)

now = datetime.datetime.now().strftime("%d-%m-%Y")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = RequestFormatter(
    '[%(asctime)s] %(remote_addr)s requested %(url)s | %(jsonresp)s: '
    '%(levelname)s in %(module)s: %(message)s')
# logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
filehandler = logging.FileHandler('API_{}.log'.format(now))
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)
 
sq = Manage_strategies()
Ex = Execution()
rts = redis_ts()
app = Flask(__name__)
turnoff = []

@app.route('/placeorder', methods=['POST'])
def placeorder():
    try: 
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
        logger.debug("orderplacement")
        return {"error" : False, "data" : [refno], "status" : "Order placed successfully."}
    except Exception as e : 
        logger.exception("Error")
        return {"error" : True, "data" : [], "status" : str(e)}

@app.route('/dependentorder', methods=['POST'])
def dependentorder():
    try: 
        global turnoff 
        data = request.json
        # print(data)
        logger.debug("orderplacement")
        if data['strategyname'] in turnoff :
            return {"error" : True, "data" : [], "status" : "Strategy is already turned off."}
        
        strategydata = sq.viewone(data['strategyname'])
        if strategydata == [] : 
            return {"error" : True, "data" : [], "status" : "Wrong strategy name"}
        
        strategydata = strategydata[0]
        qty = strategydata['qty'] if data.get("qty") == None or data.get("qty") == False else data['qty']
        splt = strategydata['freezequantity'] if data.get('split_qty') == None or data.get('split_qty') == False else data['split_qty']
        refno = [rts.incr() for i in range(len(data['token']))]
        price = [Ex.get_ltp(i)['ltp'] for i in data['token']] if not data['price'] else data['price']
        orderdict = { 
            "reftag" : refno,
            "strategyname" : data['strategyname'], 
            "token" : data['token'], 
            "transactiontype" : data['transactiontype'],
            "qty" : int(qty), 
            "split_qty" : int(splt), 
            "to_split" : False if splt > qty or splt == 1 else True, 
            "price" : price,
            "is_forward" : True if strategydata['execution_type'] == "Forward" else False, 
            "exchange" : strategydata['exchange'], 
            "reason" : "" if data.get('reason') == None or data.get('reason') == False else data['reason']
            }
        print(orderdict)
        logger.debug("dependentorder")
        t1 = threading.Thread(target = lambda : Ex.dependent_execution(orderdict)).start()
        return {"error" : False, "data" : [refno], "status" : "Order placed successfully."}
    
    except Exception as e : 
        logger.exception("Error")
        return {"error" : True, "data" : [], "status" : str(e)}
    
@app.route('/squareoff', methods=['POST'])
def squareoff():
    try : 
        global turnoff 
        data = request.json
        v = Ex.openposition(data['strategyname'])
        strategydata = sq.viewone(data['strategyname'])
        
        if data['strategyname'] in turnoff :
            return {"error" : True, "data" : [], "status" : "Strategy is already turned off."}
        
        if strategydata == [] : 
            return {"error" : True, "data" : [], "status" : "Wrong strategy name"}
        
        strategydata = strategydata[0]
        
        qtys = [abs(i['qty']) for i in v]
        is_dependent_order = all([True if i == qtys[0] else False for i in qtys])
        
        if is_dependent_order : 
            qty = qtys[0]
            splt = strategydata['freezequantity']
            refno = [rts.incr() for i in v]
            price = [Ex.get_ltp(int(i['token']))['ltp'] for i in v]
            transactiontype = ["buy" if i['qty'] < 0 else "sell" for i in v]
            orderdict = {
                "reftag" : refno,
                "strategyname" : data['strategyname'], 
                "token" : [i['token'] for i in v], 
                "transactiontype" : transactiontype,
                "qty" : int(qty), 
                "split_qty" : int(splt), 
                "to_split" : False if splt > qty or splt == 1 else True, 
                "price" : price,
                "is_forward" : True if strategydata['execution_type'] == "Forward" else False, 
                "exchange" : strategydata['exchange'], 
                "reason" : ""
                }
            print(orderdict)
        
            t1 = threading.Thread(target = lambda : Ex.dependent_execution(orderdict)).start()
            logger.debug("squareoff")
            return {"error" : False, "data" : refno, "status" : "Order placed successfully."}
    
        else : 
            refnos = []
            for i in v :
                qty = abs(i['qty'])
                splt = strategydata['freezequantity']
                refno = rts.incr()
                transactiontype = "buy" if i['qty'] < 0 else "sell"
                orderdict = {
                    "reftag" : refno,
                    "strategyname" : data['strategyname'], 
                    "token" : int(i['token']), 
                    "transactiontype" : transactiontype,
                    "qty" : int(qty), 
                    "split_qty" : int(splt), 
                    "to_split" : False if splt > qty or splt == 1 else True, 
                    "price" : Ex.get_ltp(int(i['token']))['ltp'] ,
                    "is_forward" : True if strategydata['execution_type'] == "Forward" else False, 
                    "exchange" : strategydata['exchange'], 
                    "reason" : "" 
                    }
                print(orderdict)
                refnos.append(refno)
                t1 = threading.Thread(target = lambda : Ex.singleorder(orderdict)).start()
                time.sleep(0.5)
            logger.debug("squareoff")
            return {"error" : False, "data" : refnos, "status" : "Order placed successfully."}
        
    except Exception as e :
        logger.exception("Error")
        return {"error" : True, "data" : [], "status" : str(e)}
        
@app.route("/addstrategy", methods = ['POST'])
def addstrategy(): 
    try: 
        data = request.json
        strategy = data['strategyname']
        d = sq.viewone(strategy)
        if d != [] : 
            raise Exception("Strategy Already Exists")
        sq.add_strategy(strategy, data['exectype'], data['quantity'], data['product_type'], freezequantity = data.get('freezequantity'), 
                        symbol = data.get('symbol'), grouptag = data.get('grouptag'), exchange = data.get('exchange'))
        
        logger.debug("strategy added")
        return {"error":False, "data" : [], "status" : "Strategy Added"} 
    except Exception as e:
        logger.exception("error")
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
        
        logger.debug("strategy update")
        return {"error":False, "data" : [], "status" : "Strategy Updated"} 
    except Exception as e: 
        logger.exception("error")
        return {"error":True, "data" : [], "status" : str(e)} 

@app.route("/removestrategy", methods = ['POST'])
def removestrategy(): 
    try: 
        data = request.json
        strategy = data['strategyname']
        d = sq.viewone(strategy)
        if d == [] :
            raise Exception( "Strategy doesnt exist")
        
        sq.remove_strategy(strategy)
        logger.debug("strategy removed")
        return {"error":False, "data" : [], "status" : "Strategy Removed"} 
    except Exception as e: 
        logger.exception("error")
        return {"error":True, "data" : [], "status" : str(e)} 

@app.route("/STopenpositions", methods = ['GET'])
def STopenpositions():
    try: 
        data = request.json
        logger.debug("open pos")
        return {"error" : False, "data" : Ex.openposition(data['strategyname']), "status" : "done"}
    except Exception as e : 
        logger.exception("error")
        return {"error":True, "data" : [], "status" : str(e)} 
    
@app.route("/STnetpositions", methods = ['GET'])
def STnetpositions():
    try: 
        data = request.json
        getall = True if data['strategyname'] == "" else False
        d1 = Ex.stnetpositions(data['strategyname'], getall = getall)
        logger.debug("STnetpositions")
        return {"error" : False, "data" : d1, "status" : "done"}
    except Exception as e : 
        logger.exception("error")
        return {"error":True, "data" : [], "status" : str(e)} 
    
@app.route("/STpositions", methods = ['GET'])
def STpositions():
    try: 
        data = request.json
        logger.debug("STpositions")
        return {"error" : False, "data" : sq.get_allpositions(data['strategyname']), "status" : "done"}
    except Exception as e : 
        logger.exception("error")
        return {"error":True, "data" : [], "status" : str(e)} 

@app.route("/STorders", methods = ['GET'])
def STorders():
    try: 
        data = request.json
        logger.debug("STorders")
        return {"error" : False, "data" : sq.get_orders(data['strategyname']), "status" : "done"}
    except Exception as e : 
        logger.exception("error")
        return {"error":True, "data" : [], "status" : str(e)} 

@app.route("/orderswithissues", methods = ['GET'])
def orderswithissues():
    try: 
        logger.debug("orders with issues")
        return {"error" : False, "data" : sq.orderswithissues(), "status" : "done"}
    except Exception as e : 
        logger.exception("error")
        return {"error":True, "data" : [], "status" : str(e)} 

@app.route("/positionswithissues", methods = ['GET'])
def positionswithissues():
    try: 
        logger.debug("positionswithissues")
        return {"error" : False, "data" : sq.positionswithissues(), "status" : "done"}
    except Exception as e : 
        logger.exception("error")
        return {"error":True, "data" : [], "status" : str(e)} 


@app.route("/getstrategy", methods = ['GET'])
def get_strategies():
    try: 
        stname = request.json['strategyname']
        data = sq.viewone(stname)
        if data == [] :
            raise Exception( "Strategy doesnt exist")
        logger.debug("get strategy")
        return {"error":False, "data" : data, "status" : "Data Received"} 
    except Exception as e : 
        logger.exception("error")
        return {"error":True, "data" : [], "status" : str(e)} 

@app.route("/allstrategies", methods = ['GET'])
def allstrategies():
    try: 
        logger.debug("all strategy")
        return {"error":False, "data" : sq.viewall(), "status" : "Data Received"} 
    except Exception as e : 
        logger.exception("error")
        return {"error":True, "data" : [], "status" : str(e)} 

@app.route("/api/lastupdate/<token>")
def get_last_data(token):
    try: 
        a = Ex.get_ltp(token)
        return {"Success" : True, "Data" : a, "Error": False}
    except Exception as e : 
        logger.exception("error")
        return {"Success" : False, "Error" : str(e)}

@app.route("/api/orderbook")
def get_orderbook(): 
    try : 
        return {"Success" : True, "Data" : Ex.get_orderbook(), "Error" : False}
    except  Exception as e : 
        logger.exception("error")
        return {"Success" : False, "Data" : [], "Error" : str(e)}

@app.route("/api/tradebook")
def get_tradebook(): 
    try : 
        return {"Success" : True, "Data" : Ex.get_tradebook(), "Error" : False}
    except  Exception as e : 
        logger.exception("error")
        return {"Success" : False, "Data" : [], "Error" : str(e)}

@app.route("/api/netposition")
def get_netpositions(): 
    try : 
        return {"Success" : True, "Data" : Ex.get_netpositions(), "Error" : False}
    except  Exception as e : 
        logger.exception("error")
        return {"Success" : False, "Data" : [], "Error" : str(e)}

@app.route("/stoprecon")
def stop_trade_recon():
    Ex.trades_process = False
    return {"status" : True}
    
def start_trade_recon():
    Ex.trades_process = True
    t1 = threading.Thread(target = Ex.trade_reconcile).start()
    
def active_order_threads():
    return Ex.recon_threads

def stop_recon_threads(reftag):
    Ex.recon_threads[reftag]['is_running'] = False

    
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)



