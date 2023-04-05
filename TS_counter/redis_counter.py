# -*- coding: utf-8 -*-
"""
Created on Mon Feb 27 19:25:05 2023

@author: niraj.munot
"""

import redis
import datetime

class redis_ts():
    def __init__(self):
        self.dt = datetime.date.today().strftime("%Y%m%d")
        self.r = redis.Redis()
        val = int(self.dt[-1:] + self.dt[-2:] + "0000")
        # if self.r.get(f"ts_counter_{self.dt}") == None :   
        self.r.set(f"ts_counter_{self.dt}", val)

    def incr(self):
        return self.r.incr(f"ts_counter_{self.dt}")
