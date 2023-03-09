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
import json
from Jiffy.jiffy import *
import threading
import copy 
from alerts.tele_alerts import Send_alerts

now = datetime.datetime.now().strftime("%d-%m-%Y")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
filehandler = logging.FileHandler('execution_log{}.log'.format(now))
filehandler.setFormatter(formatter)
logger.addHandler(filehandler)

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
        self.sndalert = Send_alerts()
        
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
        try: 
            producttype = ProductType.Delivery
            ordertype = OrderType.Market
            price = 0 
            transactiontype = TransactionType.Buy if orderdict['transactiontype'] == "buy" else TransactionType.Sell if orderdict['transactiontype'] == "sell" else None
            if transactiontype == None : 
                raise Exception("Incorrect Transaction type")
            
            qty = int(orderdict['qty'])
            token = int(orderdict['token'])
            segment = self.get_segment(token, orderdict['exchange'])
            try : 
                orderid = self.jf.placeorder(token, segment, ordertype, transactiontype, qty, price, producttype)
                print(orderid)
                return orderid
            except Exception as e : 
                print("Error in placing order", e)
                logger.exception("Issue in Placing Order")
                try: 
                    self.sndalert.send_alert(f"Error - {str(e)} in placeorder. \nOrderdict - {orderdict}")
                except: 
                    pass
                return None
            
        except Exception as e : 
            print("Error in placing order", e)
            logger.exception("Issue in Placing Order")
            try: 
                self.sndalert.send_alert(f"Error - {str(e)} in placeorder. \nOrderdict - {orderdict}")
            except: 
                pass
            return None
        
    def _order_exec_confirmation(self, orderdetails, orderids):
        try: 
            self.recon_threads[orderdetails['reftag']] = {"reftag" : orderdetails['reftag'], "is_running" : True}
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
                            c = self.sq.update_order(orderdetails['reftag'], orderdetails['strategyname'], i,symbol = orderdetails['symbol'],orderstatus = ordstatus['data']['OrderStatus'],
                                           exec_qty = ordstatus['data']['TradedQty'],is_done = is_done,is_exec = is_exec,placed_at = ordstatus['data']['Time']
                                           ,recon_at = datetime.datetime.now(),order_desc = ordstatus['data']['Remarks'])
                            
                            if not c["status"] : 
                                logger.critical(f"Issue in _order_exec_confirmation in SQL update order - {c} for orderdetail - {orderdetails} for orderid - {i}")
                                self.sndalert.send_alert(f"Issue in _order_exec_confirmation in SQL update order - {c} for orderdetail - {orderdetails} for orderid - {i}")
                            
                            if is_done == 1 :
                                done_ords.append(i)
                                if is_exec == 1 :   
                                    execlst.append(ordstatus['data']['TradedQty'])
                                else: 
                                    logger.critical(f"Order with orderid - {i} received orderstatus {ordstatus['data']['OrderStatus']} - orderdetails - {orderdetails} ")
                                    self.sndalert.send_alert(f"Order with orderid - {i} received orderstatus {ordstatus['data']['OrderStatus']} - orderdetails - {orderdetails} ")        
                        else : 
                            c = self.sq.update_order(orderdetails['reftag'], orderdetails['strategyname'], i,symbol = orderdetails['symbol'], orderstatus = "NOT FOUND")
                            if not c["status"] : 
                                logger.critical(f"Issue in _order_exec_confirmation in SQL update order - {c} for orderdetail - {orderdetails} for orderid - {i}")
                                self.sndalert.send_alert(f"Issue in _order_exec_confirmation in SQL update order - {c} for orderdetail - {orderdetails} for orderid - {i}")
                            
                if len(done_ords) == len(orderids):
                    self.recon_threads[orderdetails['reftag']]['is_running'] = False
                time.sleep(0.5)
            
            orderstat = "FAILED" if len(execlst) == 0 else "PARTIAL" if len(execlst) != len(orderids) else "EXECUTED" if len(execlst) == len(orderids) else None
            orderstat = "PARTIAL" if (sum(execlst) != orderdetails['qty']) and (orderstat == "EXECUTED") else orderstat 
            c = self.sq.update_position(orderdetails['reftag'], orderdetails['strategyname'], orderdetails['token'], orderstatus = orderstat, traded_qty = sum(execlst),
                                 traded_price = None, is_exec = 0 if orderstat == "FAILED" else 1, is_recon = 0, is_sqoff = 0, is_forward = 0
                                 ,exec_orders = len(execlst))
            print(c)
            if not c["status"] : 
                logger.critical(f"Issue in _order_exec_confirmation in SQL update_position - {c} for orderdetail - {orderdetails} for orderid - {i}")
                self.sndalert.send_alert(f"Issue in _order_exec_confirmation in SQL update_position - {c} for orderdetail - {orderdetails} for orderid - {i}")
            
        except Exception as e :
            logger.exception(f"Issue in _order_exec_confirmation - {str(e)} - orderdetails - {orderdetails} - orderids - {orderids}")
            self.sndalert.send_alert(f"Issue in _order_exec_confirmation - {str(e)} - orderdetails - {orderdetails} - orderids - {orderids}")
            
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
            else: 
                logger.error(f"Error in Orderplacement in singleorder Orderid not generated for orderdetails : {orderval}")
                self.sndalert.send_alert(f"Error in Orderplacement in singleorder Orderid not generated for orderdetails : {orderval}")
                
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
                c = self.sq.add_position(orderdict['reftag'], orderdict['strategyname'], orderdict['token'], tm = datetime.datetime.now(), 
                                      symbol = orderdict['symbol'], price = orderdict['price'], traded_price = None, positiontype = orderdict['transactiontype'], 
                                      qty = orderdict['qty'], traded_qty = None, orderstatus = "PLACED", is_exec = 0, is_recon = 0, is_sqoff = 0, is_forward = 0, sent_orders = len(orderids)
                                      ,exec_orders = None)
                if not c["status"] : 
                    logger.critical(f"Issue in singleorder in SQL add_position - {c} for orderdetail - {orderdict}")
                    self.sndalert.send_alert(f"Issue in singleorder in SQL add_position - {c} for orderdetail - {orderdict}")
                
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
        v = self.sq.get_positions(strategyname)
        if v == [] :
            return []
        vc = pd.DataFrame(v)
        vc['traded_qty'] = vc.apply(lambda row : row['traded_qty'] * -1 if row['positiontype'] == "sell" else row['traded_qty'], axis = 1)
        g = vc.groupby("token").agg({"traded_qty" : "sum", "symbol" : "last", "strategyname" : "last"})
        g = g.reset_index()
        g = g.sort_values(by = ['traded_qty'])
        td = []
        for i in range(len(g)): 
            if g.iloc[i]['traded_qty'] != 0 : 
               td.append({"token" : int(g.iloc[i]['token']), "qty" : int(g.iloc[i]['traded_qty']), "strategyname" : g.iloc[i]['strategyname'], "symbol" : g.iloc[i]['symbol']}) 
        return td
            
    def stnetpositions(self, strategyname, getall = False):
        if not getall : 
            x = self.sq.get_positions(strategyname)
        else : 
            x = self.sq.get_allexecpositions()
        
        if x == []:
            return []
        
        df = pd.DataFrame(x)
        g = df.groupby("token")
        td = {}
        for i in g.groups.keys():
            n = g.get_group(i)
            buytrades = n[n['positiontype'] == "buy"]
            selltrades = n[n['positiontype'] == "sell"]
            buyavg =  0 if buytrades.empty else round(sum(buytrades['traded_price'] * buytrades['traded_qty']) / sum(buytrades['traded_qty']), 5)
            sellavg = 0 if selltrades.empty else round(sum(selltrades['traded_price'] * selltrades['traded_qty']) / sum(selltrades['traded_qty']), 5)
            buyqty = 0 if buytrades.empty else sum(buytrades['traded_qty'])
            sellqty = 0 if selltrades.empty else sum(selltrades['traded_qty'])

            try : 
                ltp = self.get_ltp(i)['ltp'] #USE GET LTP HERE
            except : 
                ltp = 0

            if buyqty > sellqty:
                bookedpnl = (sellavg - buyavg) * sellqty
                remainingqty = buyqty - sellqty
                openpnl = (ltp - buyavg) * remainingqty
            
            elif sellqty > buyqty:
                bookedpnl = (sellavg - buyavg) * buyqty
                remainingqty = sellqty - buyqty
                openpnl = (sellavg - ltp) * remainingqty
            
            elif buyqty == sellqty:
                bookedpnl = (sellavg - buyavg) * buyqty
                remainingqty = 0
                openpnl = 0 #(sellavg - buyqty) * remainingqty
            
            td[str(i)] = {"token" : int(i),"buyavgprice" : float(buyavg), "buyqty" : int(buyqty), "ltp" : int(ltp), "remainingqty" : int(remainingqty), "sellavgprice" : float(sellavg), 
                     "sellqty" : float(sellqty), "bookedpnl" : float(bookedpnl), "openpnl" : float(openpnl), "totalpnl" : float(bookedpnl + openpnl)}
            
        totalbooked = sum([td[i]['bookedpnl'] for i in td])
        totalopen = sum([td[i]['openpnl'] for i in td])
        total = sum([td[i]['totalpnl'] for i in td])
        td['total'] = {"token" : "total", "bookedpnl" : float(totalbooked), "openpnl" : float(totalopen), "totalpnl" : float(total)}
        return td
        
    def dependent_execution(self, orderdict):
        orderids = {}
        for i in orderdict['reftag']:
            orderids[i] = []
            
        def orderplacement(orderval):
            for i in range(len(orderval['reftag'])):
                orderv = {
                    "reftag" : orderval['reftag'][i],
                    "price" : orderval['price'][i],
                    "token" : orderval['token'][i], 
                    "qty" : orderval['qty'],
                    "strategyname" : orderval['strategyname'],
                    "transactiontype" : orderval['transactiontype'][i],
                    "exchange" : orderval['exchange']
                    }
                print("orval",orderv)
                oid = self.placeorder(orderv)
                print("oid", oid)
                orderids[orderv['reftag']].append(oid)
                if oid != None : 
                    print(self.sq.add_order(orderv['reftag'], orderv['strategyname'], oid, orderstatus = None, token = orderv['token'], 
                                      transactiontype = orderv['transactiontype'], req_qty = orderv['qty'], exec_qty = 0, 
                                      symbol = None, is_done = 0, is_exec = 0, placed_at = None, recon_at = None, order_desc = None
                                      , is_traded = 0, requested_price = orderv['price'], traded_price = None, traded_at = None))
                else : 
                    # GIVE ALERT 
                    # ADD TO LOG FOR ERROR
                    logger.error(f"Error in Orderplacement in dependent_execution Orderid not generated for orderdetails : {orderv}")
                    self.sndalert.send_alert(f"Error in Orderplacement in dependent_execution Orderid not generated for orderdetails : {orderv}")
                time.sleep(0.1)
                
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
                
            orderdict['symbol'] = [self.get_symbol(i, orderdict['exchange']) for i in orderdict['token']]
            for i in range(len(orderdict['reftag'])):
                orderids[orderdict['reftag'][i]] = [i for i in orderids[orderdict['reftag'][i]] if i != None]
                if orderids[orderdict['reftag'][i]] != [] : 
                    orderd = {
                        "reftag" : orderdict["reftag"][i], 
                        "token" : orderdict["token"][i],
                        "qty" : orderdict["qty"],
                        "price" : orderdict["price"][i],
                        "exchange" : orderdict["exchange"],
                        "to_split" : orderdict['to_split'], 
                        "split_qty" : orderdict['split_qty'], 
                        "strategyname" : orderdict['strategyname'],
                        "transactiontype" : orderdict["transactiontype"][i],
                        "is_forward" : orderdict["is_forward"],
                        "reason" :  orderdict["reason"],
                        "symbol" : orderdict['symbol'][i] 
                        }
                    print(orderd)
                    t1 = threading.Thread(target = lambda : self._order_exec_confirmation(orderd, orderids[orderdict['reftag'][i]])).start()
                    
                    c = self.sq.add_position(orderd['reftag'], orderd['strategyname'], orderd['token'], tm = datetime.datetime.now(), 
                                          symbol = orderd['symbol'], price = orderd['price'], traded_price = None, positiontype = orderd['transactiontype'], 
                                          qty = orderd['qty'], traded_qty = None, orderstatus = "PLACED", is_exec = 0, is_recon = 0, is_sqoff = 0, is_forward = 0, sent_orders = len(orderids[orderdict['reftag'][i]])
                                          ,exec_orders = None)
                
            return orderdict, orderids
    
        else: 
            ltp = [self.get_ltp(i)['ltp'] for i in orderdict['token']]
            orderdict['symbol'] = [self.get_symbol(i, orderdict['exchange']) for i in orderdict['token']]
            
            for i in range(len(orderdict['reftag'])):
                self.sq.add_position(orderdict["reftag"][i], orderdict['strategyname'], orderdict['token'][i], tm = datetime.datetime.now(), 
                                     symbol = orderdict['symbol'][i], price = orderdict['price'][i], traded_price = ltp[i], positiontype = orderdict['transactiontype'][i], 
                                     qty = orderdict['qty'], traded_qty = orderdict['qty'], orderstatus = "EXECUTED", is_exec = 1, is_recon = 1, is_sqoff = 0, is_forward = 1, sent_orders = 1
                                     ,exec_orders = 1)
    
    def get_trades(self, startdate, enddate, strategyname = "", groupname = ""):
        if strategyname == "" and groupname == "": 
            return self.sq.get_all_trades(startdate, enddate)
    
        elif strategyname != "" : 
            return self.sq.get_trades_bystrategy(strategyname,startdate, enddate)
    
        elif groupname != "" : 
            return self.sq.get_trades_bygroup(groupname, startdate, enddate)
            
    def calculate_trades_pnl(self, a):
        for i in a :
            if i['positiontype'] == "sell" : 
                i['expected_pnl'] = (i['entryprice'] - i['exitprice']) * i['quantity']
                i['actual_pnl'] = (i['entryprice_executed'] - i['exitprice_executed']) * i['quantity']
                try: 
                    i['slippage'] = (i['actual_pnl'] - i['expected_pnl']) / i['expected_pnl'] * 100
                except:
                    i['slippage'] = 0 
                
            if i['positiontype'] == "buy" : 
                i['expected_pnl'] = (i['exitprice'] - i['entryprice']) * i['quantity']
                i['actual_pnl'] = (i['exitprice_executed'] - i['entryprice_executed']) * i['quantity']
                try: 
                    i['slippage'] = (i['actual_pnl'] - i['expected_pnl']) / i['expected_pnl'] * 100
                except: 
                    i['slippage']= 0
        
        
    def trade_push(self):
        pass
    
# =============================================================================
# print("A")
# "reftag" : orderdict['reftag'], 
# "price" : orderdict['price'],
# "token" : orderdict['token'], 
# "qty" : orderdict['split_qty'], 
# "strategyname" : orderdict['strategyname'], 
# "transactiontype" : orderdict['transactiontype'],
# "exchange" : orderdict['exchange']
# =============================================================================
# DEPENDENT ORDERS
# orderdict = {
#     "reftag" : [1236,1237],
#     "token" : [48716, 48717],
#     "qty" : 75,
#     "price" : [44, 55],
#     "exchange" : "NSEFO",
#     "to_split" : True,
#     "split_qty" : 25,
#     "strategyname" : "test_NF",
#     "transactiontype" : ["buy","sell"],
#     "is_forward" : False,
#     "reason" : None,
#     } 

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
        
    