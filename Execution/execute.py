# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 11:47:50 2023

@author: niraj.munot
"""

import logging
import datetime
import time
import config
import pandas as pd
import redis 
import ast
from sql_manager.sql_control import Manage_strategies
import numpy as np 
import json
from Jiffy.jiffy import *
import threading
import copy 

class Execution():
    def __init__(self):
        sessionid = pd.read_csv(r"{}".format(config.sessionidpath))['SessionId'].iloc[0].replace("SessionId ", "")
        self.jf = JiffyTrade(userid = config.username, sessionid = sessionid)
        self.r = redis.Redis(host = config.redis_host, port = config.redis_port)
        self.sq = Manage_strategies()
        self.tokendf = pd.read_csv(config.jiffy_scriptmaster)
        self.tokendf['trdexc'] = self.tokendf.apply(lambda row : True if row['Exchange'] in config.trading_exchange else False, axis = 1)
        self.tokendf = self.tokendf[self.tokendf['trdexc'] == True]
        self.tokendf = self.tokendf[['Token', 'Segment', "Exchange", "SecDesc"]]
        self.tokendf[['Token', 'Segment']] = self.tokendf[['Token', 'Segment']].astype(int)
        self.tokendf = self.tokendf.set_index('Token')
        self.recon_threads = {}
        self.trades_process = True
        t1 = threading.Thread(target = self.trade_reconcile).start()
        
    def get_orderbook(self):
        ob = self.r.get(config.username + "_ob")
        return ast.literal_eval(ob.decode())
    
    def get_tradebook(self):
        tb = self.r.get(config.username + "_tb")
        return ast.literal_eval(tb.decode())
    
    def get_netpositions(self):
        tb = self.r.get(config.username + "_np")
        return ast.literal_eval(tb.decode())
    
    def get_ltp(self,token):
        a = self.r.zrange(token, -1, -1)    
        a = [json.loads(i.decode('utf8').replace("'", '"')) for i in a]
        return a[0]
    
    def get_segment(self, token, exchange):
        return self.tokendf[(self.tokendf.index == token) & (self.tokendf['Exchange'] == exchange)].iloc[0]['Segment']
    
    def get_symbol(self, token, exchange):
        return self.tokendf[(self.tokendf.index == token) & (self.tokendf['Exchange'] == exchange)].iloc[0]['SecDesc']
    
    def get_orderstatus(self, ob, orderid, token): #Also check qty traded
        # ob = self.get_orderbook()
        for i in ob['Orders']:
            if i['ClientOrderNo'] == orderid and i['Token'] == token : 
                if i['OrderStatus'] == "EXECUTED" : 
                    if i['Qty'] == i['TradedQty'] : 
                        return {"is_found" : True, "data":i, "done" : True}
                    else : 
                        return {"is_found" : True, "data":i, "done" : False}
            
                if i['OrderStatus'] in ["CANCELLED", "REJECTED", "ORDER ERROR", "A.REJECT", "GATEWAY REJECT", "OMS REJECT"] :
                    print("Ordered not executed")
                    return {"is_found" : True, "data":i, "done" : True}
                
                else: 
                    print(i['OrderStatus'])
                    return {"is_found" : True, "data":i, "done" : False}
            
        return {"is_found" : False, "data":{}, "done" : False}

    def get_traded_price(self, tb, orderids, token):
        # tb = self.get_tradebook()
        tx = []
        for i in tb['Trades']:
            if i['ClientOrderNo'] in orderids and i['Token'] == token:
                tx.append(i)
        if tx != [] : 
            dff = pd.DataFrame(tx)
            return {"is_found" : True, "data" : {"tradedprice" : round(sum(dff['Price'] * dff['Qty'])/sum(dff['Qty']), 5), "tradedqty" : sum(dff['Qty']),"tradedtime" : datetime.datetime.strptime(dff['Time'].iloc[0],"%Y-%m-%d %H:%M:%S")}}
        else : 
            return {"is_found" : False, "data" : {}}
            
    def placeorder(self, orderdict):
        """
        reftag : Reference number 
        token : symbol token
        strategyname : name of the strategy
        transactiontype : buy or sell
        qty : quantity
        exchange : NSEFO, MCX, etc
        Returns
        -------
        Order ID 
 
        """ 
        producttype = ProductType.Delivery
        ordertype = OrderType.Market
        price = 0 
        transactiontype = TransactionType.Buy if orderdict['transactiontype'] == "buy" else TransactionType.Sell if orderdict['transactiontype'] == "sell" else None
        if transactiontype == None : 
            raise Exception("Incorrect Transaction type")
        
        qty = int(orderdict['qty'])
        print(qty)
        token = int(orderdict['token'])
        segment = self.get_segment(token, orderdict['exchange'])
        print("segment", segment)
        try : 
            orderid = self.jf.placeorder(token, segment, ordertype, transactiontype, qty, price, producttype)
            print(orderid)
            return orderid

        except Exception as e : 
            print("Error in placing order", e)
            return None

    def _order_exec_confirmation(self, orderdetails, orderids):
        self.recon_threads[orderdetails['reftag']] = {"reftag" : orderdetails['reftag'], "is_running" : True}
        # _exec = False
        done_ords = []
        execlst = []
        while self.recon_threads[orderdetails['reftag']]['is_running'] :
            ob = self.get_orderbook()
            for i in orderids: 
                if i not in done_ords : 
                    ordstatus = self.get_orderstatus(ob, i, orderdetails['token'])
                    if ordstatus['is_found'] : 
                        is_done = 1 if ordstatus['done'] else 0
                        is_exec = 1 if ordstatus['data']['OrderStatus'] == "EXECUTED" else 0
                        self.sq.update_order(orderdetails['reftag'], orderdetails['strategyname'], i,symbol = orderdetails['symbol'],orderstatus = ordstatus['data']['OrderStatus'],
                                       exec_qty = ordstatus['data']['TradedQty'],is_done = is_done,is_exec = is_exec,placed_at = ordstatus['data']['Time']
                                       ,recon_at = datetime.datetime.now(),order_desc = ordstatus['data']['Remarks'])
                        if is_done == 1 :
                            done_ords.append(i)
                            if is_exec == 1 :   
                                execlst.append(ordstatus['data']['TradedQty'])
                    else : 
                        self.sq.update_order(orderdetails['reftag'], orderdetails['strategyname'], i,symbol = orderdetails['symbol'], orderstatus = "NOT FOUND")
                
            if len(done_ords) == len(orderids):
                self.recon_threads[orderdetails['reftag']]['is_running'] = False
            time.sleep(0.5)
        
        orderstat = "FAILED" if len(execlst) == 0 else "PARTIAL" if len(execlst) != len(orderids) else "EXECUTED" if len(execlst) == len(orderids) else None
        orderstat = "PARTIAL" if (sum(execlst) != orderdetails['qty']) and (orderstat == "EXECUTED") else orderstat
        c = self.sq.update_position(orderdetails['reftag'], orderdetails['strategyname'], orderdetails['token'], orderstatus = orderstat, traded_qty = sum(execlst),
                             traded_price = None, is_exec = 0 if orderstat == "FAILED" else 1, is_recon = 0, is_sqoff = 0, is_forward = 0
                             ,exec_orders = len(execlst))
        print(c)
     
    def singleorder(self, orderdict):
        orderids = []
        
        def orderplacement(orderval):
            print("orval",orderval)
            oid = self.placeorder(orderval)
            print("oid", oid)
            orderids.append(oid)
            if oid != None : 
                print(self.sq.add_order(orderval['reftag'], orderval['strategyname'], oid, orderstatus = None, token = orderval['token'], 
                                  transactiontype = orderval['transactiontype'], req_qty = orderval['qty'], exec_qty = 0, 
                                  symbol = None, is_done = 0, is_exec = 0, placed_at = None, recon_at = None, order_desc = None
                                  , is_traded = 0, requested_price = orderval['price'], traded_price = None, traded_at = None))
                
        threads = []
        if not orderdict['is_forward']:
            if not orderdict['to_split']:
                t1 = threading.Thread(target = lambda : orderplacement(orderdict))
                threads.append(t1)
            else: 
                initqty = orderdict['qty']
                batches = True
                while batches : 
                    if initqty > orderdict['split_qty'] : 
                        threads.append(threading.Thread(target = lambda : orderplacement({
                            "reftag" : orderdict['reftag'], 
                            "price" : orderdict['price'],
                            "token" : orderdict['token'], 
                            "qty" : orderdict['split_qty'], 
                            "strategyname" : orderdict['strategyname'], 
                            "transactiontype" : orderdict['transactiontype'],
                            "exchange" : orderdict['exchange']})))
                        
                        initqty = initqty - orderdict['split_qty']
                        
                    elif initqty <= orderdict['split_qty'] :  

                        threads.append(threading.Thread(target = lambda : orderplacement({
                            "reftag" : orderdict['reftag'], 
                            "price" : orderdict['price'],
                            "token" : orderdict['token'], 
                            "qty" : copy.deepcopy(initqty), 
                            "strategyname" : orderdict['strategyname'], 
                            "transactiontype" : orderdict['transactiontype'],
                            "exchange" : orderdict['exchange']})))
                        batches = False                        
                 
            for i in threads : 
                i.start()
                
            for i in threads:
                i.join()
                
            orderdict['symbol'] = self.get_symbol(orderdict['token'], orderdict['exchange'])
            
            orderids = [i for i in orderids if i != None]
            if orderids != [] : 
                t1 = threading.Thread(target = lambda : self._order_exec_confirmation(orderdict, orderids)).start()
                print(self.sq.add_position(orderdict['reftag'], orderdict['strategyname'], orderdict['token'], tm = datetime.datetime.now(), 
                                      symbol = orderdict['symbol'], price = orderdict['price'], traded_price = None, positiontype = orderdict['transactiontype'], 
                                      qty = orderdict['qty'], traded_qty = None, orderstatus = "PLACED", is_exec = 0, is_recon = 0, is_sqoff = 0, is_forward = 0, sent_orders = len(orderids)
                                      ,exec_orders = None))
            
            return orderdict, orderids
    
        else:
            ltp = self.get_ltp(orderdict['token'])['ltp']
            orderdict['symbol'] = self.get_symbol(orderdict['token'], orderdict['exchange'])
            self.sq.add_position(orderdict['reftag'], orderdict['strategyname'], orderdict['token'], tm = datetime.datetime.now(), 
                                 symbol = orderdict['symbol'], price = orderdict['price'], traded_price = ltp, positiontype = orderdict['transactiontype'], 
                                 qty = orderdict['qty'], traded_qty = orderdict['qty'], orderstatus = "EXECUTED", is_exec = 1, is_recon = 1, is_sqoff = 0, is_forward = 1, sent_orders = 1
                                 ,exec_orders = 1)
            
    def _first_run_recon(self):
        pass
    
    def trade_reconcile(self):
        while self.trades_process : 
            try : 
                x = self.sq.unrecon_positions()
                if x != [] : 
                    tb = self.get_tradebook()
                    x = pd.DataFrame(x)
                    g = x.groupby("refno")
                    for i in g.groups.keys():
                        n = g.get_group(i)
                        a = self.get_traded_price(tb, n['orderid'].to_list(), n['token'].iloc[0])
                        if a['is_found']:
                            is_recon = 1 if a['data']['tradedqty'] == sum(n['exec_qty']) else 0 
                            self.sq.update_position(i, n['strategyname'].iloc[0], n['token'].iloc[0], traded_price = a['data']['tradedprice'], is_recon = is_recon)
                time.sleep(0.5)
            except Exception as e : 
                print("Trade Reconcile Exception", str(e))
    
    def openposition(self, strategyname):
        pass
    
    def strategy_Netpositions(self, strategyname):
        pass
    
    def dependent_execution(self, strategyname):
        pass
    
    def squareoff(self, strategyname):
        pass
    
    def trade_push(self):
        pass
    
# =============================================================================
# DEPENDENT ORDERS
# orderdict = {
#     "reftag" : [1236,1237],
#     "token" : [48716, 48717],
#     "qty" : 75,
#     "price" : 44,
#     "exchange" : "NSEFO",
#     "to_split" : True,
#     "split_qty" : 25,
#     "strategyname" : "test_NF",
#     "transactiontype" : "sell",
#     "is_forward" : False,
#     "reason" : None,
#     } 
# =============================================================================

# =============================================================================
# orderdict = {
#     "reftag",
#     "token",
#     "qty",
#     "exchange",
#     "to_split",
#     "split_qty",
#     "strategyname",
#     "transactiontype",
#     "is_forward",
#     "reason",
#     }
# =============================================================================
        
    