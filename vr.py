# -*- coding:utf-8 -*- 
'''
Created on 2016/10/10
Calculating Volume Rate
@author: Cruise Huang

'''
import os.path
from datetime import datetime,date,time,timedelta
from functools import partial
from multiprocessing import Value,Lock
from multiprocessing.pool import Pool

import pandas as pd

from tushare.stock import trading as td
from tushare.stock import cons as ct
from tushare.util import dateu as du

import dataHis 

#Not used
def get_tick_multi(symbols=None, date=None):
    ct._write_head()
    print(du.get_now()+' 批量获取分笔数据')
    if (isinstance(symbols, list) or isinstance(symbols, set) or
        isinstance(symbols, tuple) or isinstance(symbols, pd.Series)):
        if(date is None):
            multiFunc = partial(td.get_today_ticks,
                                retry_count=retry_count, pause=pause)
        else:
            multiFunc = partial(td.get_tick_data,
                                date=date, retry_count=retry_count, pause=pause)
        with Pool(16) as p:
            results = p.map(multiFunc, symbols)
    

def timeDiff(t1,t2):
    d = datetime.today().date()
    return datetime.combine(d,t1) - datetime.combine(d,t2) 

def tradeTime(curTime):
    if(curTime >= time(hour=9,minute=15) and curTime < time(hour=9,minute=30)):
        delta = timedelta(minutes=1)
    elif(curTime > time(hour=9,minute=30) and curTime <= time(hour=11,minute=30)):
        delta = timeDiff(curTime, time(hour=9,minute=30))
    elif(curTime > time(hour=11,minute=30) and curTime <= time(hour=13,minute=00)):
        delta = timedelta(hours=2)
    elif(curTime > time(hour=13,minute=00) and curTime <= time(hour=15,minute=00)):
        delta = timeDiff(curTime, time(hour=13,minute=00)) + timedelta(hours=2)
    elif(curTime > time(hour=15,minute=00)):
        delta = timedelta(hours=4)
    else:
        return None

    return delta.total_seconds() // 60


def calc_vol_rate(rate = 2.0):
    loaded = pd.read_csv(ct.CSV_DIR+'stocks_his_lastday.csv', dtype='str')
    
    v5 = dict();
    ma10 = dict();
    for i in range(len(loaded)):
        code = loaded.ix[i]['code']
        v5[code] = float(loaded.ix[i]['v_ma5'])
        ma10[code] = float(loaded.ix[i]['ma10']) * 1.05 #当日开盘价在前日MA10的5%（人工总结，可调整）以内

    current = dataHis.get_today_all_multi()
    select = []
    for j in range(len(current)):
        key = str(current.ix[j]['code'])
        if(key is None):
            continue
        try:
            row = current.ix[j]
            now = datetime.now().time()
            vr = float(row['volume']) * 60 * 4 / tradeTime(now) / v5[key] / 100 
            price = float(row['trade'])
            if ( vr > rate                                                     ##量比 > 2
                 and price > 0.0 and price <= ma10[key]                        ##当日开盘价在前日MA10的5%以内
                 and row['changepercent'] > 2.0 and row['changepercent'] < 7.0 ##涨幅 2%到7%
                 and (row['nmc'] / row['trade']) <= 30000):                    ##流通盘小于3亿
                data = {'code':key,
                        'name':row['name'],
                        'cp':row['changepercent'],
                        'price':row['trade'],
                        'vol_rate': '%.2f' % vr }
                ct._write_msg(data['code'] + ':' + data['vol_rate'] + '\n')
                select.append(data)
        except KeyError as e:
            continue

    pd.DataFrame(select, dtype='str').to_csv(ct.CSV_DIR+'select_by_vol_rate.csv')
    return select

def main():
    path2Codes = 'C:/Users/Cruis/home/investment/stocks_his_lastday.csv'
    if(os.path.exists(path2Codes) == False):
        print('Ref File Not existed!')
        return

    calc_vol_rate()
  
 
if __name__ == '__main__':
    main()