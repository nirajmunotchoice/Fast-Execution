# -*- coding: utf-8 -*-
"""
Created on Wed Feb 22 10:44:21 2023

@author: niraj.munot
"""

import mysql.connector
from config import sql_host, sql_port, sql_database, sql_user, sql_password
import datetime

class Manage_strategies():
    def __init__(self): 
        self.host = sql_host
        self.user = sql_user
        self.password = sql_password
        self.database = sql_database
        self.strategiesdata = "strategies_data"
        self.positions = "positions1"
        self.trades = "trades"
        self.orderbook = "refidorderbook1"
        self.db = mysql.connector.connect(host=self.host, user=self.user, password=self.password, database = self.database)
        self.cursor = self.db.cursor(buffered=True)
    
    def add_strategy(self, name, exectype, quantity, product_type, freezequantity = 1, symbol = "", grouptag = "", exchange = ""): 
        try : 
            sql = f"""INSERT INTO {self.strategiesdata} (strategyname, executiontype, quantity, producttype, freezequantity,
            symbol, grouptag, exchange) VALUES ('{name}', '{exectype}', '{quantity}', '{product_type}', '{freezequantity}','{symbol}', '{grouptag}', '{exchange}')"""
            self._executor(sql)
            return {"status" : True, "exception" : False, "function_name" : "Manage_strategies add_strategy"}
        except Exception as e : 
            return {"status" : True, "exception" : str(e), "function_name" : "Manage_strategies add_strategy"}
        
    def remove_strategy(self, name): 
        try : 
            sql = f"""DELETE FROM {self.strategiesdata} WHERE strategyname = '{name}'"""
            self._executor(sql)
            return {"status" : True, "exception" : False, "function_name" : "Manage_strategies remove_strategy"}
        except Exception as e : 
            return {"status" : False, "exception" : str(e), "function_name" : "Manage_strategies remove_strategy"}
            
    def update_strategy(self, name, exectype = None, quantity = None, product_type = None, freezequantity = None, symbol = None, grouptag = None, exchange = None):
        try : 
            dic = {"executiontype" : exectype, 
                   "quantity" : quantity, 
                   "producttype" : product_type,
                   "freezequantity" : freezequantity, 
                   "symbol" : symbol, 
                   "grouptag" : grouptag, 
                   "exchange" : exchange}
            
            query_string = ""
            for i in dic:
                if dic[i] != None:
                    query_string = query_string + " " + i + f" = '{dic[i]}'" + ","
            
            if query_string != "":
                query_string = query_string[1:-1]
                sql = f"""UPDATE {self.strategiesdata} SET {query_string} WHERE  strategyname='{name}'"""
                self._executor(sql)
                return {"status" : True, "exception" : False, "function_name" : "Manage_strategies update_strategy"}
            else: 
                return {"status" : False, "exception" : "query name incomplete", "function_name" : "Manage_strategies update_strategy"}
        except Exception as e :
            return {"status" : True, "exception" : str(e), "function_name" : "Manage_strategies update_strategy"}
        
    def viewall(self): 
        v = self._reader(f"SELECT * FROM {self.strategiesdata}")
        n = {}
        for i in v : 
            n[i[0]] = {"strategyname":i[0], "execution_type" : i[1], "qty" : i[2], "product_type" : i[3],
                       "freezequantity" : i[4], "symbol" : i[5], "grouptag" : i[6], "exchange" : i[7]}
        return n
     
    def viewone(self, name): 
        v = self._reader(f"SELECT * FROM {self.strategiesdata} WHERE strategyname = '{name}'")
        columns = ['strategyname', 'execution_type', "qty", "product_type", "freezequantity", "symbol", "grouptag", "exchange"]
        return [{col : i[columns.index(col)] for col in columns} for i in v]
        
    def add_order(self, refno, strategyname, orderid, orderstatus = None, token = None, transactiontype = None, req_qty = None, exec_qty = None, symbol = None, is_done = None, is_exec = None, placed_at = None, recon_at = None, order_desc = None, is_traded = None, requested_price = None, traded_price = None, traded_at = None):
        try: 
            dic = {"refno" : refno, 
                   "strategyname" : strategyname, 
                   "orderid" : orderid,
                   "orderstatus" : orderstatus,
                   "token" : token, 
                   "transactiontype" : transactiontype,
                   "req_qty" : req_qty,
                   "exec_qty" : exec_qty, 
                   "symbol" : symbol,
                   "is_done" : is_done,
                   "is_exec" : is_exec, 
                   "placed_at" : placed_at, 
                   "recon_at" : recon_at, 
                   "order_desc" : order_desc,
                   "is_traded" : is_traded, 
                   "requested_price" : requested_price,
                   "traded_price" : traded_price,
                   "traded_at" : traded_at
                   }
            
            query_string = ""
            values = []
            for i in dic : 
                if dic[i] != None : 
                    query_string = query_string + i + ", "
                    values.append(dic[i])
            
            if query_string != "" : 
                query_string = query_string[:-2]
                valno = ""
                for _ in range(len(values)):
                    valno = valno + "%s, "
                valno = valno[:-2]
                sql = f"INSERT INTO {self.orderbook} ({query_string}) VALUES ({valno})"
                val = tuple(values)
                self._executor(sql, val)
                return {"status" : True, "exception" : False, "function_name" : "Manage_strategies add_order"}
            else: 
                return {"status" : False, "exception" : "query name incomplete", "function_name" : "Manage_strategies add_order"}
        
        except Exception as e : 
            return {"status" : False, "exception" : str(e), "function_name" : "Manage_strategies add_order"}
            
    def update_order(self, refno, strategyname, orderid, orderstatus = None, token = None, transactiontype = None, req_qty = None, exec_qty = None, symbol = None, is_done = None, is_exec = None, placed_at = None, recon_at = None, order_desc = None, is_traded = None, requested_price = None, traded_price = None, traded_at = None):
        try : 
            dic = {
                   "orderstatus" : orderstatus,
                   "token" : token, 
                   "transactiontype" : transactiontype,
                   "req_qty" : req_qty,
                   "exec_qty" : exec_qty, 
                   "symbol" : symbol,
                   "is_done" : is_done,
                   "is_exec" : is_exec, 
                   "placed_at" : placed_at, 
                   "recon_at" : recon_at, 
                   "order_desc" : order_desc,
                   "is_traded" : is_traded, 
                   "requested_price" : requested_price,
                   "traded_price" : traded_price,
                   "traded_at" : traded_at
                   }
            
            query_string = ""
            for i in dic:
                if dic[i] != None:
                    query_string = query_string + " " + i + f" = '{dic[i]}'" + ","
            
            if query_string != "":
                query_string = query_string[1:-1]
                sql = f"""UPDATE {self.orderbook} SET {query_string} WHERE refno='{refno}' AND strategyname = '{strategyname}' AND orderid = '{orderid}'"""
                self._executor(sql)
                return {"status" : True, "exception" : False, "function_name" : "Manage_strategies update_order"}
            
            else: 
                return {"status" : False, "exception" : "query name incomplete", "function_name" : "Manage_strategies update_order"}
        
        except Exception as e : 
            return {"status" : False, "exception" : str(e), "function_name" : "Manage_strategies update_order"}
    
    def add_position(self, refno, strategyname, token, tm = None, symbol = None, price = None, traded_price = None, positiontype = None, qty = None, traded_qty = None, orderstatus = None, is_exec = None, is_recon = None, is_sqoff = None, is_forward = None, sent_orders = None,exec_orders = None):
        # vals = ["refno", "strategyname", "tm", "symbol", "price", "traded_price", "positiontype", "qty", "traded_qty", "token", "orderstatus", "is_exec", "is_recon",
        #         "is_sqoff", "is_forward", "sent_orders", "exec_orders"]
        try: 
            dic = {"refno" : refno, 
                   "strategyname" : strategyname, 
                   "token" : token, 
                   "tm" : tm, 
                   "symbol" : symbol,
                   "price" : price, 
                   "traded_price" : traded_price,
                   "positiontype" : positiontype, 
                   "qty" : qty,
                   "traded_qty" : traded_qty, 
                   "orderstatus" : orderstatus,
                   "is_exec" : is_exec, 
                   "is_recon" : is_recon, 
                   "is_sqoff" : is_sqoff,
                   "is_forward" : is_forward,
                   "sent_orders" : sent_orders,
                   "exec_orders" : exec_orders
                   }
            
            query_string = ""
            values = []
            for i in dic : 
                if dic[i] != None : 
                    query_string = query_string + i + ", "
                    values.append(dic[i])
            
            if query_string != "" : 
                query_string = query_string[:-2]
                valno = ""
                for _ in range(len(values)):
                    valno = valno + "%s, "
                valno = valno[:-2]
                sql = f"INSERT INTO {self.positions} ({query_string}) VALUES ({valno})"
                val = tuple(values)
                self._executor(sql, val)
                return {"status" : True, "exception" : False, "function_name" : "Manage_strategies add_position"}
            else: 
                return {"status" : False, "exception" : "query name incomplete", "function_name" : "Manage_strategies add_position"}
        
        except Exception as e : 
            return {"status" : False, "exception" : str(e), "function_name" : "Manage_strategies add_position"}

    def update_position(self,refno, strategyname, token, tm = None, symbol = None, price = None, traded_price = None, positiontype = None, qty = None, traded_qty = None, orderstatus = None, is_exec = None, is_recon = None, is_sqoff = None, is_forward = None, sent_orders = None,exec_orders = None):
        try : 
            dic = {
                   "tm" : tm, 
                   "symbol" : symbol,
                   "price" : price, 
                   "traded_price" : traded_price,
                   "positiontype" : positiontype, 
                   "qty" : qty,
                   "traded_qty" : traded_qty, 
                   "orderstatus" : orderstatus,
                   "is_exec" : is_exec, 
                   "is_recon" : is_recon, 
                   "is_sqoff" : is_sqoff,
                   "is_forward" : is_forward,
                   "sent_orders" : sent_orders,
                   "exec_orders" : exec_orders
                   }
            
            query_string = ""
            for i in dic:
                if dic[i] != None:
                    query_string = query_string + " " + i + f" = '{dic[i]}'" + ","
            
            if query_string != "":
                query_string = query_string[1:-1]
                sql = f"""UPDATE {self.positions} SET {query_string} WHERE refno='{refno}' AND strategyname = '{strategyname}'"""
                self._executor(sql)
                return {"status" : True, "exception" : False, "function_name" : "Manage_strategies update_position"}
            
            else: 
                return {"status" : False, "exception" : "query name incomplete", "function_name" : "Manage_strategies update_position"}
        
        except Exception as e : 
            return {"status" : False, "exception" : str(e), "function_name" : "Manage_strategies update_position"}
    
    def unrecon_positions(self):
        a = self._reader(f"""SELECT {self.positions}.refno, {self.orderbook}.orderid, {self.positions}.strategyname, {self.positions}.token, {self.orderbook}.is_exec, {self.orderbook}.exec_qty FROM {self.orderbook} INNER JOIN {self.positions} ON {self.positions}.refno = {self.orderbook}.refno WHERE {self.positions}.is_exec = '1' AND {self.positions}.is_recon = '0' """)
        vs = ['refno', "orderid", "strategyname", "token", "is_exec", "exec_qty"]
        vz = [{v : i[vs.index(v)] for v in vs} for i in a]
        return vz
# =============================================================================
#         v = self._reader(f"SELECT * FROM {self.positions} WHERE is_exec = '1' AND is_recon = '0'")
#         return v
#         vals = ["refno", "strategyname", "tm", "symbol", "price", "traded_price", "positiontype", "qty", "traded_qty", "token", "orderstatus", "is_exec", "is_recon",
#                 "is_sqoff", "is_forward", "sent_orders", "exec_orders"]
#         # dictc = {i : v1}
#         v1 = [list(i) for i in v]
#         v2 = [{val : i[vals.index(val)] for val in vals} for i in v1]
# =============================================================================
# =============================================================================
#         n = {}
#         for i in v : 
#             n[i[0]] = {"strategyname":i[0], "execution_type" : i[1], "qty" : i[2], "product_type" : i[3],
#                        "freezequantity" : i[4], "symbol" : i[5], "grouptag" : i[6], "exchange" : i[7]}
#         return n
# =============================================================================

    def get_positions(self, strategyname):
        a = self._reader(f"""SELECT * FROM {self.positions} WHERE strategyname = '{strategyname}'""")
        vals = ["refno", "strategyname", "tm", "symbol", "price", "traded_price", "positiontype", "qty", "traded_qty", "token", "orderstatus", "is_exec", "is_recon",
                        "is_sqoff", "is_forward", "sent_orders", "exec_orders"]
        return [{val : i[vals.index(val)] for val in vals} for i in a]
        
    def get_orderids(self, refno, strategyname, token):
        v = self._reader(f"SELECT orderid FROM {self.orderbook} WHERE strategyname = '{strategyname}' AND refno = '{refno}' AND token = '{token}'")
        return v
        
    def _executor(self, sqlmsg, val = None):
        db = mysql.connector.connect(host=self.host, user=self.user, password=self.password, database = self.database)
        cursor = db.cursor()
        if val == None : 
            cursor.execute(sqlmsg)        
        else : 
            cursor.execute(sqlmsg, val)
        db.commit()    
        cursor.close()
        db.close()
    
    def _reader(self, sqlmsg): 
        db = mysql.connector.connect(host=self.host, user=self.user, password=self.password, database = self.database)
        cursor = db.cursor()
        cursor.execute(sqlmsg)
        a = cursor.fetchall()
        cursor.close()
        db.close()
        return a 
    