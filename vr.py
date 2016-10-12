# -*- coding:utf-8 -*- 
'''
Created on 2016/10/10
Calculating Volume Rate
@author: Cruise Huang

'''
import os,os.path
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

def readNews():
    path2News = datetime.now().strftime('news/%Y%m%d.csv')
    news = pd.read_csv(ct.CSV_DIR+path2News, dtype='str')
    codesInNews = []
    for i in range(len(news)):
        code = news.ix[i]['head'].split('.')[0]
        codesInNews.append(code)    

    return codesInNews

def calc_vol_rate(rate = 2.0):
    loaded = pd.read_csv(ct.CSV_DIR+'stocks_his_lastday.csv', dtype='str')
    news = readNews()
    
    stock = dict()
    for i in range(len(loaded)):
        code = loaded.ix[i]['code']
        data = dict()
        data['price'] = float(loaded.ix[i]['close'])
        data['volume'] = float(loaded.ix[i]['volume'])
        data['per'] = float(loaded.ix[i]['p_change'])
        data['ma5'] = float(loaded.ix[i]['ma5'])
        data['ma10'] = float(loaded.ix[i]['ma10'])
        data['ma20'] = float(loaded.ix[i]['ma20']) 
        data['v5'] = float(loaded.ix[i]['v_ma5'])
        data['v10'] = float(loaded.ix[i]['v_ma10'])
        data['v20'] = float(loaded.ix[i]['v_ma20'])
        stock[code] = data


    current = dataHis.get_today_all_multi()
    selected = []
    for j in range(len(current)):
        key = str(current.ix[j]['code'])
        if(key is None):
            continue
        try:
            row = current.ix[j]
            now = datetime.now().time()
            vr = float(row['volume']) * 60 * 4 / tradeTime(now) / stock[key]['v5'] / 100 
            price = float(row['trade'])
            if ( vr > rate and price > 0 and (row['nmc'] / row['trade']) <= 800000   ##量比 > 2 ;流通盘小于80亿
                 and row['changepercent'] > 2.0 and row['changepercent'] < 7.0):     ##涨幅 2%到7%
                sel = {'code':key,
                        'name':row['name'],
                        'cp':row['changepercent'],
                        'price':row['trade'],
                        'vol_rate': '%.2f' % vr }
                ct._write_msg(" \n%s %s: %s" % (sel['code'],sel['name'],sel['vol_rate']))
                
                ##更多条件
                if( price <= stock[key]['ma10'] * 1.05                         ##当日开盘价在前日MA10的5%以内
                    and stock[key]['volume'] < stock[key]['v10'] * 1.5          ##前一天没有放巨量
                   ):
                    sel['note'] = '精选'
                    ct._write_msg(" <==精选")

                if( key in news):
                    sel['news'] = '利好'
                    ct._write_msg(" <==利好")
                
                selected.append(sel)
                

        except KeyError as e:
            continue

    path = ct.CSV_DIR + datetime.now().strftime('results/%Y%m%d_%H%M/')
    os.mkdir(path)   
    pd.DataFrame(selected, dtype='str').to_csv(path+'select_vr.csv')

def main():
    path2Ref = ct.CSV_DIR+'stocks_his_lastday.csv'
    if(os.path.exists(path2Ref) == False):
        print('Ref File Not existed!')
        return

    resultPath = ct.CSV_DIR+'results/'
    if(os.path.exists(resultPath) == False):
        os.mkdir(resultPath)

    calc_vol_rate()
  
 
if __name__ == '__main__':
    main()