# -*- coding: utf-8 -*-
"""
Created on Thu Jun  2 11:59:33 2022

@author: niraj.munot
"""

import pandas as pd
import numpy as np
import datetime
import requests
from envparse import env
import json
import time
import Jiffy.aes as aes #pip install pycryptodome
from base64 import b64encode, b64decode
from requests.auth import HTTPBasicAuth
from csv import writer
import math
import enum

url = "https://jiffy.choiceindia.com/api/"

endpoints = {
    "login" : url + "OpenAPI/Login",
    "placeorder" : url + "OpenAPI/NewOrder",
    "modifyorder" : url + "OpenAPI/ModifyOrder",
    "cancelorder" : url + "OpenAPI/CancelOrder",
    "orderbook" : url + "OpenAPI/OrderBook",
    "tradebook" : url + "OpenAPI/TradeBook",
    "netposition" : url + "OpenAPI/NetPosition",
    "ordermessages" : url + "OpenAPI/OrderMessages",
    "marketstatus" : url + "OpenAPI/MarketStatus",
    "userprofile" : url + "OpenAPI/UserProfile",
    "multitouchline" : url + "OpenAPI/MultipleTouchline",
    "fundsview" :  url + "OpenAPI/FundsView",
    "chartapi" : url + "cm/Graph/GetChartHistory"
    }

# _member_names_
class Segments(enum.IntEnum): 
    NSECash = 1
    NSEFNO = 2
    BSECash = 3
    MCXNFO = 5
    MCXSpot = 6
    NCDEXNFO = 7
    NCDEXSpot = 9
    CDSNFO = 13
    CDSSpot = 14
    @staticmethod
    def list():
        return list(map(lambda c: c.value, Segments))

class OrderType(enum.Enum):
    Limit = "RL_LIMIT"
    Market = "RL_MKT"
    StoplossLimit = "SL_LIMIT"
    StoplossMarket = "SL_MKT"

class TransactionType(enum.IntEnum):
    Buy = 1
    Sell = 2

class ProductType(enum.Enum):
    Intraday = "M"
    Delivery = "D"
    AMO = "AM"
    
class JiffyTrade : 
    def __init__(self, userid = "", password = "",sessionid = "", userid_fordata = "guestUser",sessionid_fordata = "9CC2F807AE" ):
        
        self.key = "2b7e151628aed2a6abf7158809cf4f3c"
        self.IV = "3ad77bb40d7a3660"
        self.reqsession = requests.Session()
        self.timeout = 15
        self.curdate = datetime.datetime.now().strftime("%d%b%Y")
        
        if password != "" : 
            self.body = {
                "UserId" : userid,
                "Pwd" : b64encode(aes.encrypt(password, self.key, self.IV)),
                }
        self.headers = {
            "VendorId" : "QUANTT",
            "VendorKey" : "6E4VPDBGOORDZY6P4D8BR6MBJ2TBCSSW",
            }    
        # self.sessionid = ""
        self.paisa = 100
        if sessionid == "":
            self.sessionid = ""
            self._login()
        else : 
            self.sessionid = sessionid
            self._login()
            
        self.diff_delta = (datetime.datetime(1980, 1, 1, 0, 0, 0) - datetime.datetime(1970, 1, 1, 0, 0, 0)).total_seconds()
        self.userid_fordata = userid_fordata
        self.sessionid_fordata = sessionid_fordata
        self.diff_delta = (datetime.datetime(1980, 1, 1, 0, 0, 0) - datetime.datetime(1970, 1, 1, 0, 0, 0)).total_seconds()
        
    def _login(self): 
        try: 
            if self.sessionid == "" : 
                self.sessionid = open("sessionid.txt".format(self.curdate),"r").readlines()[0]
            
            print("using existing")
        except :
            resp =  requests.post(endpoints['login'], json=self.body, headers=self.headers)
            if resp.json()['Status']=='Success':
                self.sessionid = resp.json()['Response']['SessionId']
                f= open("sessionid_{}.txt".format(self.curdate),"w")
                f.write(self.sessionid)
                f.close()
        if self.sessionid != "":
            self.headers['Authorization'] = "SessionId " +  self.sessionid
        else: raise Exception (resp.json()['Response']['LogonMessage'])

    def placeorder(self, token, segment, ordertype, transactiontype, qty, price, product_type,triggerprice = 0.0):
        body = {"SegmentId": int(segment),"Token": token,"OrderType": ordertype.value,"BS": transactiontype.value,"Qty": qty,
                "DisclosedQty": 0,"Price": int(price*self.paisa),"TriggerPrice": int(triggerprice*self.paisa), "Validity": 1,"ProductType": product_type.value,
                "IsEdisReq": True} #orderid,, "ClientOrderNo": orderid
        return self._request("POST", endpoints['placeorder'], body)
    
    def modifyorder(self, orderid, exchangeorderno, gatewayorderno, token, segment, ordertype,transactiontype, qty, price,product_type, triggerprice = 0  ):
        body = {"ClientOrderNo" : orderid,"ExchangeOrderNo": exchangeorderno,"GatewayOrderNo":  gatewayorderno,"SegmentId": int(segment),"Token": token,
                "OrderType": ordertype.value,"BS": transactiontype.value,"Qty": qty,
                "DisclosedQty": 0,"Price": int(price*self.paisa),"TriggerPrice":int(triggerprice*self.paisa) , "Validity": 1,"ProductType": product_type.value,
                "IsEdisReq": True}
        print(body)
        return  self._request("POST",endpoints['modifyorder'],body)
    
    
    def cancelorder(self,exchangeordertime, orderid,exchangeorderno,gatewayorderno, token, segment, ordertype, transactiontype,qty, price,product_type, triggerprice = 0):
        body = {"ExchangeOrderTime": "1339066814","ClientOrderNo": orderid,"ExchangeOrderNo": exchangeorderno,"GatewayOrderNo": gatewayorderno,"SegmentId": int(segment),"Token": token,
         "OrderType": ordertype.value,"BS": transactiontype.value,"Qty": qty,"DisclosedQty": 0,"Price": price,"TriggerPrice": triggerprice
         ,"Validity": 0,"ProductType": product_type.value,"IsEdisReq": True}
        
        return  self._request("POST",endpoints['cancelorder'],body)

    def orderbook(self):
        return self._request("GET", endpoints['orderbook'])
    
    def tradebook(self): 
        return self._request("GET", endpoints['tradebook'])
    
    def netpositions(self):
        return self._request("GET", endpoints['netposition'])
    
    def ordermessages(self, orderid = "") :
        body = {"Id" : orderid}
        return self._request("POST", endpoints['ordermessages'], body)
    
    def fundsview(self):
        return self._request("GET", endpoints['fundsview'])
    
    def marketstatus(self): 
        return self._request("GET", endpoints['marketstatus'])
    
    def userprofile(self):
        return self._request("GET", endpoints['userprofile'])
    
    def _request(self, method, url, body = None, is_headers = True): 
        try :  
            if is_headers :
                resp = self.reqsession.request(method, url, json = body, headers = self.headers, timeout= self.timeout) 
            
            if not is_headers :
                resp = self.reqsession.request(method, url, json = body, timeout = self.timeout)
            
            if resp.status_code != 200:
                raise requests.HTTPError(resp.text)
            
            if resp.json()['Status']=='Success':
                return resp.json()['Response']
            
            if resp.json()['Status'] != 'Success' : 
                raise Exception(resp.json()['Reason'])
            
            else: return resp.json()
            
        except Exception as e :
            raise e
            
    
    
    
    # data = r.json()["Response"]['lstChartHistory']
    # tmplst = [i.split(',') for i in data]
    # df = pd.DataFrame(tmplst, columns = ['Datetime', "Open", "High", "Low", "Close", "Volume", "OpenInterest"])
    # df = df.apply(pd.to_numeric)
    # df[['Open', "High", "Low", "Close", "Volume", "OpenInterest"]] = df[['Open', "High", "Low", "Close", "Volume", "OpenInterest"]] / 100.0
    # df['Datetime'] = pd.to_datetime(df['Datetime']+diff_delta, unit = "s")
    # df['Ts'] = df['Datetime'] + diff_delta
    # df['Datetime'] = df['Ts'].apply(lambda x: datetime.datetime.fromtimestamp(x,pytz.timezone("UTC")).replace(tzinfo=None))
    # df['Datetime'] = df['Datetime'].apply(lambda x :x.replace(second=0))

    
# class ChartApi(JiffyTrade) :
#     def __init__(self, userid = "guestUser", sessionid = "9CC2F807AE"): 
#         self.diff_delta = (datetime.datetime(1980, 1, 1, 0, 0, 0) - datetime.datetime(1970, 1, 1, 0, 0, 0)).total_seconds()
#         self.userid = userid
#         # super().__init__()
        
#     def get_date_tm(self, date) : 
#         d1 = datetime.datetime(1980, 1, 1, 0, 0, 0)
#         dt = datetime.datetime.combine(datetime.date.today(), datetime.time(hour=0, minute=0))
#         return (dt-d1).total_seconds()

#     def _fetchdata(self,segment,token,startdate,enddate, interval):
#         pass
    
    
    
