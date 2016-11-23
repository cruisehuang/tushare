# -*- coding:utf-8 -*- 
'''
Created on 2016/10/10
Review the selected
@author: Cruise Huang

'''
import os
from datetime import datetime,timedelta
from functools import partial
from multiprocessing.pool import Pool

import pandas as pd


import config as cfg
import utils

def _review(code,dateFile):
    path2Stock = cfg.PATH_2_HIS_DATA + code+'.csv';
    df = pd.read_csv(path2Stock, dtype='str', encoding='utf8')
    sorted_df = df.sort_values('date')

    utils.msg('\rCalculating: '+code)
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

def review(dateFile, suffix = None):
    fileName = dateFile
    if(suffix is not None):
        fileName += suffix

    filePath = cfg.PATH_2_REVIEW+fileName+'.csv'
    if(utils.pathExists(filePath) == False):
        return

    selected = pd.read_csv(filePath, dtype='str', encoding='utf8')

    multiFunc = partial(_review,dateFile=dateFile)
    with Pool(16) as p:
        results = p.map(multiFunc, selected['1_code'])

    pd.DataFrame(results, dtype='str').to_csv(cfg.PATH_2_REVIEW+'review_raw_'+fileName+'.csv', encoding='utf8')



def main():
    reviewDay = utils.now()
    dayCount = 0

    while dayCount <= 6:
        if(utils.isHoliday(reviewDay.date().isoformat()) == False):
            date = reviewDay.date().isoformat()
            review(dateFile=date)
            review(dateFile=date, suffix='_op')
            review(dateFile=date, suffix='_cont')
            
        reviewDay = reviewDay - timedelta(days=1)
        dayCount+=1


if __name__ == '__main__':
    main()