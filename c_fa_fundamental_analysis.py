# -*- coding:utf-8 -*- 
'''
Created on 2016/10/10
Fundamental Analysis
@author: Cruise Huang

'''
import os,os.path
from datetime import datetime,date,time,timedelta
from functools import partial
from multiprocessing.pool import Pool

import pandas as pd

from tushare.stock import fundamental
from tushare.stock import cons as ct

PATH_2_FUND_DATA = ct.CSV_DIR+'fundData/'

def get_save_3y_fund_data():    
    for y in [2014,2015,2016]:
        for q in [1,2,3,4]:
            df = fundamental.get_report_data(y,q)
            if df is not None:
                df.to_csv(PATH_2_FUND_DATA+str(y)+'_'+str(q)+'.csv', encoding='utf8')

def split_data_by_codes():
    df = pd.DataFrame()
    for file in os.listdir(PATH_2_FUND_DATA):
        if(file.endswith('.csv')):
            data = pd.read_csv(PATH_2_FUND_DATA + file, dtype='str', encoding='utf8')
            data.insert(loc=1,column='quarter',value=file.split('.')[0])
            df = df.append(data)

    dictList = dict()
    for i,r in df.iterrows():
        code = r['code']
        ct._write_msg('\rCollecting:' + code)
        if(code not in  dictList):
            dictList[code] = pd.DataFrame()
            dictList[code] = dictList[code].append(r)
        else:
            dictList[code] = dictList[code].append(r)

    ct._write_msg('\n')
    for k,v in dictList.items():
        ct._write_msg('\rWriting: '+ k)
        pd.DataFrame(v, dtype='str').to_csv(PATH_2_FUND_DATA+'byCodes/'+k+'.csv', encoding='utf8')
    


def main():
    #get_save_3y_fund_data()
    split_data_by_codes()
      
 
if __name__ == '__main__':
    main()