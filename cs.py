# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 17:06:22 2022

@author: niraj.munot
"""

import pandas as pd 
from executionenginepy import ExecutionEngine
import datetime
import numpy as np
import time
from indicator.supertrend import supertrend2
import math 
import threading

class Logic(): 
    def __init__(self): 
        self.ex = ExecutionEngine()
        self.tokendf = pd.read_csv(r"C:\Users\Chmain.CCAPLHOCPU011\Desktop\Execution_Manager\tokens\tokensdf.csv")
        self.tokendf = self.tokendf[self.tokendf['Symbol'] == "BANKNIFTY"]
        self.tokendf['Expiry'] = pd.to_datetime(self.tokendf['Expiry'], errors='coerce')
        self.current_expiry = self._get_exp()
        self.tokendf = self.tokendf[self.tokendf['Expiry'] == self.current_expiry]
        self.tokendf['StrikePrice'] = (self.tokendf['StrikePrice']/100).astype(int)
        self.index = 26009
        self.symbol = "BANKNIFTY" if self.index == 26009 else "NIFTY" if self.index == 26000 else None
        self.size = 100 if self.index == 26009 else 50 if self.index == 26000 else None
        self.strategyname = "CS-ST-BNF"
        self.timeseq = pd.date_range(start = datetime.datetime.combine(datetime.date.today(), datetime.time(9,30)), end = datetime.datetime.combine(datetime.date.today(), datetime.time(15,15))
                                     , freq = "15min")
        self.first_run = False
        self.trade = True
        self.previousdata = self.ex.get_ohlc_fromTD(self.index, "index", tf = 15, duration = "7 D")
        
    def _get_exp(self):
        exp_list = list(self.tokendf.groupby("Expiry").groups.keys())
        exp_list.sort()
        for i in exp_list : 
            if i.date() > datetime.datetime.now().date() : 
                return i

    def get_token(self,strike, typ):
        return self.tokendf[(self.tokendf['StrikePrice'] == strike) & (self.tokendf['OptionType'] == typ)].iloc[0]['Token']
    
    def get_live(self,token): 
        ab = True
        while ab : 
            try : 
                ltp = self.ex.get_live(token)
                ab = False
            except Exception as e :
                print(e)
                print("Issue with getting Live Price")
                time.sleep(1)
        return ltp
        
    def get_ohlc(self, token, tf):
        ab = True
        while ab : 
            try : 
                ltp = self.ex.get_ohlc(token, tf = tf)
                ab = False
            except :
                pass
        return ltp
    
    def get_ohlc_TD(self, token,exc, tf = 15, dur = "6 D"):
        ab = True
        while ab :
            try : 
                ab = False
                return self.ex.get_ohlc_fromTD(token, exc, tf = tf,duration = dur)
            except :
                time.sleep(1)
                pass
        
    def place_order(self,token, ltp, typ):
        try:
            print(token, ltp, typ)
            self.ex.placeorder(self.strategyname, int(token), float(ltp), typ)
        except Exception as e:
            print(e)
            
    def place_spread(self, ltp, typ):
        # ltp = self.get_live(self.index)
        if typ == "CE" : 
            cst = int(math.ceil(ltp/100)*100)
            scst = cst + 500
            tk1 = self.get_token(scst, "CE")
            tk2 = self.get_token(cst, "CE")
            self.ex.startstreaming([tk1,tk2], "opt")
            time.sleep(1)
            tk1_ltp = self.get_live(tk1)['ltp']
            tk2_ltp = self.get_live(tk2)['ltp']
            if (tk2_ltp - tk1_ltp) > 7.5 : 
                self.place_order(tk1, tk1_ltp, "buy")
                time.sleep(1)
                self.place_order(tk2, tk2_ltp, "sell")
            else : 
                print("Spread less than 7.5 Rs.")
        
        if typ == "PE" : 
            cst = int(math.floor(ltp/100)*100)
            scst = cst - 500
            tk1 = self.get_token(scst, "PE")
            tk2 = self.get_token(cst, "PE")
            self.ex.startstreaming([tk1,tk2], "opt")
            time.sleep(1)
            tk1_ltp = self.get_live(tk1)['ltp']
            tk2_ltp = self.get_live(tk2)['ltp']
            if (tk2_ltp - tk1_ltp) > 7.5 : 
                
                self.place_order(tk1, tk1_ltp, "buy")
                time.sleep(1)
                self.place_order(tk2, tk2_ltp, "sell")
            else : 
                print("Spread less than 7.5 Rs.")
    
    def check_if_position_exists(self, get_spd = False):
        # ck = self.ex.openpositions(self.strategyname)
        # if ck['data'] == [] : 
        #     return False
        #     print("No Previous Positions")
        # else : 
        #     ab = False
        #     while not ab : 
        #         try: 
        #             dets = {}
        #             if get_spd : 
        #                 for i in ck['data'] :
        #                     if i['positiontype'] == "buy": 
        #                         buysp = i['entryprice_executed']
        #                         dets['buytoken'] = i['token']    
                                
        #                     if i['positiontype'] == "sell": 
        #                         sellsp = i['entryprice_executed']
        #                         dets['selltoken'] = i['token']
        #                 dets['spread'] = sellsp - buysp
        #                 ab = True
        #                 return dets
        #             else: 
        #                 ab = True
        #                 return True
        #         except Exception as e :
        #             print(e)
        #             print("retry in check for open pos")
        #             time.sleep(0.5)
        #     else : 
        #         return True
        ab = False
        while not ab :
            try: 
                ck = self.ex.openpositions(self.strategyname)
                if ck['data'] == []:
                    print("No previous Positions")
                    ab = True
                    return False
                
                else:
                    dets = {}
                    if get_spd : 
                        for i in ck['data']:
                            if i['positiontype'] == "buy": 
                                buysp = i['entryprice_executed']
                                dets['buytoken'] = i['token']
                            
                            if i['positiontype'] == "sell" : 
                                sellsp = i['entryprice_executed']
                                dets['selltoken'] = i['token']
                        
                        dets['spread'] = sellsp - buysp
                        ab = True
                        return dets
                    else : 
                        ab = True
                        return True
            except Exception as e :
                print(e)
                print("Retry in Check for Open POS")
                time.sleep(0.5)
            
    def check_for_sl_target(self, dets):
        self.check_sl = True
        while self.check_sl : 
            spread = self.get_live(dets['selltoken'])['ltp'] - self.get_live(dets['buytoken'])['ltp']
            if spread > dets['spread']*1.5 :
                print("SL HIT")
                try:
                    print(f"Spread is {spread} which is greater than {dets['spread']*1.5}")
                except: 
                    pass
                
                if self.check_if_position_exists() : 
                    self.ex.squareoffcs(self.strategyname)
                    self.check_sl = False
                else : 
                    self.check_sl = False
            
            if spread < 7.5 : 
                print("Target Achieved")
                if self.check_if_position_exists() : 
                    self.ex.squareoffcs(self.strategyname)
                    self.check_sl = False
                else : 
                    self.check_sl = False
            
            if datetime.datetime.now().time() >= datetime.time(15,29) : 
                self.check_sl = False
                print("Stopped SL check and turning off ALGO")
            time.sleep(1)
    
    def _placement_conformation(self, last = False): 
        x = 0 
        rrun = True
        while x < 10  and rrun : 
            try : 
                v = self.check_if_position_exists(get_spd=True)
                if v != False : 
                    if not last : 
                        t1 = threading.Thread(target = lambda : self.check_for_sl_target(v)).start()
                    else:
                        self.check_for_sl_target(v)
                    rrun = False
                else : 
                    x = x + 1
                print(x)
                if x >= 10 : 
                    print("Order not in database")
                time.sleep(2)
            except Exception as e : 
                print(e)
                time.sleep(1)
                x = x + 1
                if x >= 10 : 
                    print("NOT GETTING ORFER CONFORMATION")
        
    def check_st(self):
        print(datetime.datetime.now())
        # df = self.get_ohlc(self.index, tf = 15)
        # df = self.get_ohlc_TD(self.index, "index")
        df = self.previousdata.append(self.ex.get_ohlc(self.index, tf = 15, today = True)) 
        # df = df.dropna() 
        print(datetime.datetime.now())
        df['supertrend'] = supertrend2(df,3,10)
        if (df.iloc[-1]['close'] > df.iloc[-1]['supertrend']) and (df.iloc[-2]['close'] < df.iloc[-2]['supertrend']) : 
            print("ST Up Trend")
            if self.check_if_position_exists() : 
                try : 
                    self.check_sl = False
                    time.sleep(3)
                    self.ex.squareoffcs(self.strategyname)
                    time.sleep(3)
                except Exception as e :
                    print(e)
                    print("Unable to square off existing position")
            
            print("Placing NEW CS")
            self.place_spread(df.iloc[-1]['supertrend'], "PE")
            time.sleep(2)
            t1 = threading.Thread(target = lambda : self._placement_conformation()).start()
        
        if (df.iloc[-1]['close'] < df.iloc[-1]['supertrend']) and (df.iloc[-2]['close'] > df.iloc[-2]['supertrend']) : 
            print("ST Down Trend")
            if self.check_if_position_exists() : 
                try : 
                    self.check_sl = False
                    time.sleep(3)
                    self.ex.squareoffcs(self.strategyname)
                    time.sleep(2)
                except Exception as e :
                    print(e)
                    print("Unable to square off existing position")
            
            print("Placing NEW CS")
            self.place_spread(df.iloc[-1]['supertrend'], "CE")
            t1 = threading.Thread(target = lambda : self._placement_conformation()).start()
    
    def _find_next_run(self, dm): 
        return self.timeseq[self.timeseq.searchsorted(dm)]

    def run(self):
        nxt = self._find_next_run(datetime.datetime.now())
        print("NEXT RUN AT", nxt)

        while self.trade : 
            if datetime.datetime.now().time() >= datetime.time(9,20) and not self.first_run : 
                print("First RUN")
                ck = self.check_if_position_exists(get_spd=True)
                
                if ck != False : 
                    t1 = threading.Thread(target = lambda : self.check_for_sl_target(ck)).start()
                    self.ex.startstreaming([ck['selltoken'], ck['buytoken']], "opt")
                if ck == False : 
                    print("NO Open Trades")
                self.first_run = True

                nxt = self._find_next_run(datetime.datetime.now())
                print("NEXT RUN AT", nxt)
            
                # self._placement_conformation(last = True)
                
            if datetime.datetime.now() >= nxt and self.trade : 
                print("STARTED AT",datetime.datetime.now())
                self.check_st()
                dm = datetime.datetime.now()
                print("END AT",dm)
                if dm.time() < datetime.time(15,15):
                    nxt = self._find_next_run(dm)
            
            if datetime.datetime.now().time() >= datetime.time(15,15): 
                self.trade = False
                self.check_sl = False
                time.sleep(3)
                ck = self.check_if_position_exists(get_spd=True)
                if ck != False : 
                    self.check_for_sl_target(ck)
                else : 
                    print("No Open positions shutting Down Algo!")
                    
            time.sleep(0.5)
            
            
            
                
        
        
        
        
        
        
        
        
        