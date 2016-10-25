# -*- coding:utf-8 -*- 
'''
Created on 2016/10/10
龙虎榜
@author: Cruise Huang

'''
import os,os.path
from datetime import datetime,date,time,timedelta
from functools import partial
from multiprocessing.pool import Pool

import pandas as pd

from tushare.stock import billboard as bb
from tushare.stock import cons as ct

PATH_2_BILLBOARD_DATA = ct.CSV_DIR+'billboard/'


def main():
    #df = bb.top_list(date='2016/10/21')
    #df = bb.cap_tops()
    df = bb.broker_tops(10)
    for i,r in df.iterrows():
        if(r['bcount']>10):
            print(r)    
      
 
if __name__ == '__main__':
    main()