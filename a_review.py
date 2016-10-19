# -*- coding:utf-8 -*- 
'''
Created on 2016/10/10
Review the selected
@author: Cruise Huang

'''
import os,os.path
from datetime import datetime,date,time,timedelta
from functools import partial
from multiprocessing.pool import Pool

import pandas as pd

from tushare.stock import cons as ct
from tushare.util import dateu as du

PATH_2_HIS_DATA = ct.CSV_DIR+'historyData/'
PATH_2_REVIEW = ct.CSV_DIR+'review/'

def _review(code,dateFile):
    path2Stock = PATH_2_HIS_DATA + code+'.csv';
    df = pd.read_csv(path2Stock, dtype='str',encoding='gbk')
    sorted_df = df.sort_values('date')

    ct._write_msg('\rCalculating: '+code)
    date = datetime.strptime(dateFile, '%Y-%m-%d')

    result = dict()
    result['0_code'] = code
    result['1_date'] = dateFile
    price = 9999

    closes = []
    for i,r in sorted_df.iterrows():
        rDate = r['date']
        if(datetime.strptime(rDate, '%Y-%m-%d') < date or 
           datetime.strptime(rDate, '%Y-%m-%d') - date > timedelta(days=6)):
            continue

        if(datetime.strptime(rDate, '%Y-%m-%d') == date):
            price = float(r['open'])
            result['2_price'] = price

        result[str(9-i)+'_0_date'] = rDate
        result[str(9-i)+'_1_close'] = r['close']
        result[str(9-i)+'_2_per'] = '%.2f' % ((float(r['close'])-price)/price * 100)

    return result

def review(dateFile):
    selected = pd.read_csv(PATH_2_REVIEW+dateFile+'.csv', dtype='str',encoding='gbk')

    multiFunc = partial(_review,dateFile=dateFile)
    with Pool(16) as p:
        results = p.map(multiFunc, selected['code'])

    pd.DataFrame(results, dtype='str').to_csv(PATH_2_REVIEW+'review_raw_'+dateFile+'.csv')



def main():
    today = datetime.today()
    dayCount = 0
    while dayCount <= 5:
        if(du.is_holiday(today.strftime('%Y/%m/%d')) == False):
            review(today.date().isoformat())
            
        today = today - timedelta(days=1)
        dayCount+=1

 
if __name__ == '__main__':
    main()