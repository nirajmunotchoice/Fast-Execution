# -*- coding: utf-8 -*-
"""
Created on Tue Feb 14 10:04:44 2023

@author: niraj.munot
"""
 
from threading import Thread
from threading import Lock
import datetime

class ThreadSafeCounter():
    def __init__(self):
        self._counter = int(datetime.date.today().strftime("%Y%m%d") + "000001")  
        self._lock = Lock()
 
    def value(self):
        with self._lock:
            self._counter += 1
            return self._counter

