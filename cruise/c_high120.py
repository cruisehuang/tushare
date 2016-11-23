# -*- coding:utf-8 -*- 
'''
Created on 2016/10/10
Calculating New High in 120 days
@author: Cruise Huang

'''
import os,os.path
from datetime import datetime,date,time,timedelta
from functools import partial
from multiprocessing.pool import Pool

import pandas as pd

import config as cfg
import utils

PATH_2_H120 = cfg.PATH_2_RESULTS + 'high_120/'

def _calc(file):
    path2Stock = cfg.PATH_2_HIS_DATA + file;
    df = pd.read_csv(path2Stock, dtype='str', encoding='utf8')
    if len(df)<5:
        return

    high = 0.0
    low = 9999.0
    price = float(df.ix[0].close)
    code = df.ix[0].code
    utils.msg('\rCalculating: '+code)

    for i in range(1,len(df)):
        if(i>120):
            break

        data = df.ix[i]
        if float(data.high) > high:
            high = float(data.high)
        if float(data.low) < low:
            low = float(data.low)

    if (high - low) / low < 0.3 and price >=high:
        print('\nFound: '+code+'\n')
        return code


def calc_high_120():
    stocks = os.listdir(cfg.PATH_2_HIS_DATA)
    
    multiFunc = partial(_calc)
    with Pool(16) as p:
        results = p.map(multiFunc, stocks)

    path = PATH_2_H120 + datetime.now().strftime('%Y%m%d/')
    os.mkdir(path)   
    pd.DataFrame([r for r in results if r is not None], dtype='str').to_csv(path+'select_h120.csv', encoding='utf8')

def main():
    utils.getPath(PATH_2_H120)

    calc_high_120()
  
 
if __name__ == '__main__':
    main()