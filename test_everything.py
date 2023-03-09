# -*- coding: utf-8 -*-
"""
Created on Wed Feb 22 10:47:50 2023

@author: niraj.munot
"""

from sql_manager.sql_control import Manage_strategies
import datetime
import pandas as pd 
sq = Manage_strategies()

# sq.add_position(12345, "test_NF", 48716, tm = datetime.datetime.now(), symbol = None, price = None, traded_price = None, positiontype = None, qty = None, traded_qty = None, orderstatus = None, is_exec = None, is_recon = None, is_sqoff = None, is_forward = None, sent_orders = None,exec_orders = None)
# sq.update_position(1235, "test_NF", 48716, tm = datetime.datetime.now())
# v = sq.get_positions()
# c = sq.get_orderids(1236, "test_NF", 48716)

a = sq._reader(f"""SELECT {sq.positions}.refno, {sq.orderbook}.orderid, {sq.positions}.strategyname, {sq.positions}.token, {sq.orderbook}.is_exec FROM {sq.orderbook} INNER JOIN {sq.positions} ON {sq.positions}.refno = {sq.orderbook}.refno WHERE {sq.positions}.is_exec = '1' AND {sq.positions}.is_recon = '0' """)
a = sq.orderswithissues()
b = sq.positionswithissues()

break
# sq.add_strategy("test_NF", "Forward", 100, "Delivery", grouptag= "test")

sq.update_strategy("test_NF", quantity= 55, grouptag = "test1", exchange="NSEFO")
sq.remove_strategy("test_NF")

tm.get_data(['BANKNIFTY', 'BANKNIFTY-I'], starttime = datetime.datetime(2023,1,1, 9, 15), endtime = datetime.datetime(2023,1,1, 9, 30))
{"BANKNIFTY" : pd.DataFrame(), "BANKNIFTY-I" : pd.DataFrame()}

import time
st = time.time()
sq.viewone("test_NF")
ed = time.time()
print(ed - st)

sq.viewall()

st = time.time()
ex.get_orderbook()
ed = time.time()
print(ed - st)

sq.add_order(12345, "test_NF", 222)
import datetime
sq.update_order(12345, "test_NF", 222, is_exec = 0)

from Execution.execute import Execution
ex = Execution()
ex.trades_process = False
ex.get_ltp(26009)

orderdict = {
    "reftag" : [1012, 1013],
    "token" : [48739, 48732],
    "qty" : 75,
    "price" : [4, 7],
    "exchange" : "NSEFO",
    "to_split" : True,
    "split_qty" : 50,
    "strategyname" : "test_NF",
    "transactiontype" : ["sell", "sell"],
    "is_forward" : False,
    "reason" : None,
    }

import time
st = time.time()
v = ex.dependent_execution(orderdict)
ed = time.time()
print(ed - st)

orderdict = {
    "reftag" : 1007,
    "token" : 48739,
    "qty" : 25,
    "price" : 4,
    "exchange" : "NSEFO",
    "to_split" : False,
    "split_qty" : False,
    "strategyname" : "test_NF",
    "transactiontype" : "sell",
    "is_forward" : False,
    "reason" : None,
    }

orderdict = {
    'reftag': 3, 
    'strategyname': 'test_NF', 
    'token': 48717, 
    'transactiontype': 'buy', 
    'qty': 50, 
    'split_qty': 25,
    'to_split': True, 
    'price': 3.35,
    'is_forward': True, 
    'exchange': 'NSEFO',
    'reason': ''
    }
import time
st = time.time()
v = ex.singleorder(orderdict)
ed = time.time()
print(ed - st)
ob = ex.jf.orderbook()
ob = ob['Orders']
ex._order_exec_confirmation(orderdict, v[1])

ex.get_orderstatus(ob, 200000464, 48716)

ords = [200000299, 100000329, 200000300]
ex.get_traded_price(tb, ords, 48716)

ords = [100000361, 100000360,200000334]
import pandas as pd 
tb = ex.get_tradebook()
token = 48683
oid = 200000230

ex.get_traded_price(tb, oid, token)

ex.order_push(orderdict, ords)
ex._order_exec_confirmation(orderdict, ords)

buyorderids = [100000394, 100000395]
sellorderids = [100000396, 200000385, 100000397]
import copy
a = 1
id(a)
id(a*1)
id(copy.deepcopy(a))
ex.get_orderstatus(200000345, orderdict)

import threading 
import time
import random

class resp_from_thread():
    def __init__(self):
        self.reruns = 100

    def get_number(self):
        return random.randint(100, 999)

    def func(self) : 
        lst = []
        def getno():
            time.sleep(0.5)
            lst.append(self.get_number())
        threads = []
        for _ in range(self.reruns):
            threads.append(threading.Thread(target = getno))
        for i in threads : 
            i.start()
        for i in threads:
            i.join()
        return lst

r = resp_from_thread()
st = time.time()
f = r.func() 
ed = time.time()
print(ed-st, f)

strategyname = "test_NF"
vc = pd.DataFrame(sq.get_positions(strategyname))
vc['traded_qty'] = vc.apply(lambda row : row['traded_qty'] * -1 if row['positiontype'] == "sell" else row['traded_qty'], axis = 1) 
g = vc.groupby("token").agg({"traded_qty" : "sum", "symbol" : "last", "strategyname" : "last"})
g = g.reset_index()
td = []
for i in range(len(g)): 
    if g.iloc[i]['traded_qty'] != 0 : 
       td.append({"token" : g.iloc[i]['token'], "qty" : g.iloc[i]['traded_qty'], "strategyname" : g.iloc[i]['strategyname'], "symbol" : g.iloc[i]['symbol']}) 


x = sq.unrecon_positions()
x = pd.DataFrame(x)
g = x.groupby("refno")
for i in g.groups.keys():
    n = g.get_group(i)
    a = ex.get_traded_price(tb, n['orderid'].to_list(), n['token'].iloc[0])
    if a['is_found']:
        is_recon = 1 if a['data']['tradedqty'] == sum(n['exec_qty']) else 0 
        sq.update_position(i, n['strategyname'].iloc[0], n['token'].iloc[0], traded_price = a['data']['tradedprice'], traded_qty = a['data']['tradedqty'], is_recon = is_recon)

trades = pd.DataFrame()
x = sq.get_positions("test_NF")
df = pd.DataFrame(x)
g = df.groupby("token")

stname = "test_NF"
for i in g.groups.keys():
    n = g.get_group(i)
    n['tm'] = pd.to_datetime(n['tm'])
    n = n.sort_values(by = ['tm'])
    # n = n.append(n)
    n = n.reset_index(drop = True, inplace = False)
    # n.at[4, "refno"] = 2809
    # n.at[5, "refno"] = 2810
    # n.at[6, "refno"] = 2811
    # n.at[7, "refno"] = 2812
    # n = n.set_index("refno") 
    # n.at[2805, "qty"] = 200
    # n.at[2806, "qty"] = 100
    vnnn = n.copy(deep = True)
    # for na in range(len(n)) :
    unchanged = False
    while not unchanged and not n.empty: 
        v = n.iloc[0]
        postype = v['positiontype']
        qty = v['qty']
        
        td = {"strategyname" : stname,"entryprice" : v['price'], "token" : v['token'], "entrytime" : v['tm'], "entryprice_exeucted": v['traded_price']}
        xn = n[(n.index != v.name) & (n['positiontype'] != postype)]
        print(qty)
        ch = False
        xa = 0
        for xa in range(len(xn)):
            if qty != 0 :
                vxz = xn.iloc[xa]
                td['exittime'] = vxz['tm']
                td['exitprice'] = vxz['price']
                if qty == vxz['qty']:
                    td['qty'] = qty
                    qty = 0
                    n.drop([v.name, vxz.name], axis = 0, inplace = True)
                    
                elif qty > vxz['qty']:
                    td['qty'] = vxz['qty']
                    qty = qty - vxz['qty']
                    n.at[v.name, "qty"] = qty
                    n.drop([vxz.name], axis = 0, inplace = True)
                    
                elif qty < vxz['qty']:
                    n.at[vxz.name, "qty"] = vxz['qty'] - qty
                    n.drop([v.name], axis = 0, inplace = True)
                    td['qty'] = qty
                    qty = 0
                print(qty)
                ch = True
                trades = trades.append(td, ignore_index= True)
        unchanged = True if ch == False else unchanged
        time.sleep(0.5)

lsttrades = ['id', "strategyname", "entrytime", "exittime", "symbol", "entryprice", "entryprice_executed", "exitprice", "exitprice_executed", "positiontype", 
             "quantity", "token", "date", "exit_reason", "forward_test"]

n = n.append(n)
n = n.reset_index()
n.at[2, "refno"] = 2801
n.at[3, "refno"] = 2802
n = n.set_index("refno")
n.at[2801, "positiontype"] = "sell"
n.at[2802, "positiontype"] = "sell"
vnnn['traded_qty'] = vnnn.apply(lambda row : row['traded_qty'] * -1 if row['positiontype'] == "sell" else row['traded_qty'], axis = 1) 
g1 = vnnn.groupby("token").agg({"traded_qty" : "sum", "token" : "last", "symbol" : "last", "strategyname" : "last"})

# =============================================================================
# GET AVERAGE OF TRADES
# =============================================================================


trades = pd.DataFrame(td)
x = sq.get_positions("test_NF")
df = pd.DataFrame(x)
df = df.iloc[1:]
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
    ltp = 6.05 #USE GET LTP HERE
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
    
    td[i] = {"token" : i,"buyavgprice" : buyavg, "buyqty" : buyqty, "ltp" : ltp, "remainingqty" : remainingqty, "sellavgprice" : sellavg, 
             "sellqty" : sellqty, "bookedpnl" : bookedpnl, "openpnl" : openpnl, "totalpnl" : bookedpnl + openpnl}
        
    # td[i] = {'buyaverage' : sum(buytrades['traded_price'] * buytrades['traded_qty']) , "buyqty" : }
    # n1 = n.groupby('token')   

x = pd.DataFrame(ex.stnetpositions("test_NF"))
x = x.transpose()

# =============================================================================
# Positions to trades
# =============================================================================

import time
trades = pd.DataFrame()
x = sq.get_positions("test_NF")
df = pd.DataFrame(x)
g = df.groupby("token")

opentrades = pd.DataFrame()

stname = "test_NF"

for i in g.groups.keys():
    n = g.get_group(i)
    n['tm'] = pd.to_datetime(n['tm'])
    n = n.sort_values(by = ['tm'])
    n = n.reset_index(drop = True, inplace = False)
    vnnn = n.copy(deep = True)
    unchanged = False
    while not unchanged and not n.empty: 
        v = n.iloc[0]
        postype = v['positiontype']
        qty = v['traded_qty']
        td = {"strategyname" : stname,"entryprice" : v['price'], "entrytime" : v['tm'], "entryprice_executed": v['traded_price'],
              "positiontype" : postype, "token" : v['token'], "symbol" : v['symbol'], "forward_test" : False if v['is_forward'] == 0 else True}
        xn = n[(n.index != v.name) & (n['positiontype'] != postype)]
        ch = False
        xa = 0
        for xa in range(len(xn)):
            if qty != 0 :
                vxz = xn.iloc[xa]
                if qty == vxz['traded_qty']:
                    td['quantity'] = qty
                    td['date'] = vxz['tm'].date()
                    td['exittime'] = vxz['tm']
                    td['exitprice'] = vxz['price']
                    td['exitprice_executed'] = vxz['traded_price']
                    qty = 0
                    n.drop([v.name, vxz.name], axis = 0, inplace = True)
                    
                elif qty > vxz['traded_qty']:
                    td['quantity'] = qty
                    td['date'] = vxz['tm'].date()
                    td['exittime'] = vxz['tm']
                    td['exitprice'] = vxz['price']
                    td['exitprice_executed'] = vxz['traded_price']
                    qty = qty - vxz['traded_qty']
                    n.at[v.name, "traded_qty"] = qty
                    n.at[v.name, "qty"] = qty
                    n.drop([vxz.name], axis = 0, inplace = True)
                    
                elif qty < vxz['traded_qty']:
                    n.at[vxz.name, "qty"] = vxz['traded_qty'] - qty
                    n.at[vxz.name, "traded_qty"] = vxz['traded_qty'] - qty
                    n.drop([v.name], axis = 0, inplace = True)
                    td['quantity'] = qty
                    td['date'] = vxz['tm'].date()
                    td['exittime'] = vxz['tm']
                    td['exitprice'] = vxz['price']
                    td['exitprice_executed'] = vxz['traded_price']
                    qty = 0
                print(qty)
                ch = True
                trades = trades.append(td, ignore_index= True)
        unchanged = True if ch == False else unchanged
        time.sleep(0.5)
    
    if not n.empty:
        opentrades = opentrades.append(n)
        
lsttrades = ['id', "strategyname", "entrytime", "exittime", "symbol", "entryprice", "entryprice_executed", "exitprice", "exitprice_executed", "positiontype", 
             "quantity", "token", "date", "exit_reason", "forward_test"]

for i in range(len(trades)):
    c = trades.iloc[i]    
    sq.add_trade(c['strategyname'], c['entrytime'], c['symbol'], c['entryprice'], c['entryprice_executed'], c['positiontype'], int(c['quantity']), int(c['token']), 
                 c['exittime'], c['exitprice'], c['exitprice_executed'], "", c['date'])

# =============================================================================
# ALERTS
# =============================================================================
from alerts.tele_alerts import Send_alerts

snd = Send_alerts()

snd.send_alert("test ALERT")



c = sq.get_all_trades(datetime.date(2023,1,1), datetime.date(2023,3,5))
