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

from tushare.stock import cons as ct

PATH_2_HIS_DATA = ct.CSV_DIR+'historyData/'

def _calc(file):
    path2Stock = PATH_2_HIS_DATA + file;
    df = pd.read_csv(path2Stock, dtype='str',encoding='gbk')
    if len(df)<5:
        return

    high = 0.0
    low = 9999.0
    price = float(df.ix[0].close)
    code = df.ix[0].code
    ct._write_msg('\rCalculating: '+code)

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
    stocks = os.listdir(PATH_2_HIS_DATA)
    
    multiFunc = partial(_calc)
    with Pool(16) as p:
        results = p.map(multiFunc, stocks)

    path = ct.CSV_DIR + datetime.now().strftime('results/%Y%m%d_%H%M/')
    os.mkdir(path)   
    pd.DataFrame([r for r in results if r is not None], dtype='str').to_csv(path+'select_h120.csv')

def main():

    resultPath = ct.CSV_DIR+'results/'
    if(os.path.exists(resultPath) == False):
        os.mkdir(resultPath)

    calc_high_120()
  
 
if __name__ == '__main__':
    main()