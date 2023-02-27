# -*- coding: utf-8 -*-
"""
Created on Wed Feb 22 10:47:50 2023

@author: niraj.munot
"""

from sql_manager.sql_control import Manage_strategies

sq = Manage_strategies()

sq.add_position(12345, "test_NF", 48716, tm = datetime.datetime.now(), symbol = None, price = None, traded_price = None, positiontype = None, qty = None, traded_qty = None, orderstatus = None, is_exec = None, is_recon = None, is_sqoff = None, is_forward = None, sent_orders = None,exec_orders = None)
sq.update_position(1235, "test_NF", 48716, tm = datetime.datetime.now())
v = sq.get_positions()
c = sq.get_orderids(1236, "test_NF", 48716)

a = sq._reader(f"""SELECT {sq.positions}.refno, {sq.orderbook}.orderid, {sq.positions}.strategyname, {sq.positions}.token, {sq.orderbook}.is_exec FROM {sq.orderbook} INNER JOIN {sq.positions} ON {sq.positions}.refno = {sq.orderbook}.refno WHERE {sq.positions}.is_exec = '1' AND {sq.positions}.is_recon = '0' """)

break
sq.add_strategy("test_NF", "Forward", 100, "Delivery", grouptag= "test")

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
ex.get_ltp(26009)

orderdict = {
    "reftag" : 1236,
    "token" : 48716,
    "qty" : 75,
    "price" : 44,
    "exchange" : "NSEFO",
    "to_split" : True,
    "split_qty" : 25,
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
g = vc.groupby("token").agg({"traded_qty" : "sum", "token" : "last", "symbol" : "last", "strategyname" : "last"})

x = sq.unrecon_positions()
x = pd.DataFrame(x)
g = x.groupby("refno")
for i in g.groups.keys():
    n = g.get_group(i)
    a = ex.get_traded_price(tb, n['orderid'].to_list(), n['token'].iloc[0])
    if a['is_found']:
        is_recon = 1 if a['data']['tradedqty'] == sum(n['exec_qty']) else 0 
        sq.update_position(i, n['strategyname'].iloc[0], n['token'].iloc[0], traded_price = a['data']['tradedprice'], traded_qty = a['data']['tradedqty'], is_recon = is_recon)
            