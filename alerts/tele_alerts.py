# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 14:36:41 2022

@author: niraj.munot
"""

import requests
from queue import Queue
import time
import threading

class Send_alerts():
    def __init__(self): 
        self.token = "5635254217:AAEHkPmSdAitTbbwNGWMqe9hlDu_osyv9BE"
        self.chat_id = "-665831662"
        self.url = "https://api.telegram.org/bot{}/sendMessage?chat_id={}&text={}"
        self.orderlist = Queue(maxsize = 500)
        self.alert_sender = True
        threading.Thread(target = self.execute_alerts).start()
        
    def send_alert(self, message):
        self.orderlist.put(message)
        return True
    
    def execute_alerts(self):
        while self.alert_sender:
            while not self.orderlist.empty()  :
                message = self.orderlist.get()
                try : 
                    ax = requests.get(self.url.format(self.token, self.chat_id, message), timeout = 5)
                    if ax.status_code == 200 : 
                        pass
                    else : 
                        raise Exception("Status code : {ax.status_code}")    
                except Exception as e:
                    print(e)
                time.sleep(1)
            time.sleep(0.5)
            
            
            