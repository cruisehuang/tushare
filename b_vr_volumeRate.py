# -*- coding:utf-8 -*- 
'''
Created on 2016/10/10
Calculating Volume Rate
@author: Cruise Huang

'''
import os,os.path
from datetime import datetime,date,time,timedelta
from functools import partial
from multiprocessing.pool import Pool

import pandas as pd

from tushare.stock import trading as td
from tushare.stock import cons as ct
from tushare.util import dateu as du

import a_dc_dataCollect as dc

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
    if(curTime >= time(hour=9,minute=15) and curTime < time(hour=9,minute=31)):
        delta = timedelta(minutes=1)
    elif(curTime >= time(hour=9,minute=31) and curTime <= time(hour=11,minute=30)):
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
    path2News = ct.CSV_DIR + datetime.now().strftime('news/%Y%m%d.csv')
    if(os.path.exists(path2News) == False):
        print(path2News)
        return []

    news = pd.read_csv(path2News, dtype='str', encoding='utf8')
    codesInNews = []
    for i in range(len(news)):
        code = news.ix[i]['head'].split('.')[0]
        codesInNews.append(code)    

    return codesInNews

def readBillboard():
    path2BB= ct.CSV_DIR + datetime.now().strftime('billboard/%Y%m%d_merged.csv')
    if(os.path.exists(path2BB) == False):
        return []

    bb = pd.read_csv(path2BB, dtype='str', encoding='utf8')
    bbDict = dict()
    for i,r in bb.iterrows():
        code = r['code']
        bbDict[code] = r

    return bbDict

def readDataLastday():
    loaded = pd.read_csv(ct.CSV_DIR+'stocks_his_lastday.csv', dtype='str', encoding='utf8')
    stock = dict()

    for i in range(len(loaded)):
        code = loaded.ix[i]['code']
        data = dict()
        #data['price'] = float(loaded.ix[i]['close'])
        data['volume'] = float(loaded.ix[i]['volume'])
        #data['per'] = float(loaded.ix[i]['p_change'])
        #data['ma5'] = float(loaded.ix[i]['ma5'])
        data['ma10'] = float(loaded.ix[i]['ma10'])
        #data['ma20'] = float(loaded.ix[i]['ma20']) 
        data['v5'] = float(loaded.ix[i]['v_ma5'])
        data['v10'] = float(loaded.ix[i]['v_ma10'])
        #data['v20'] = float(loaded.ix[i]['v_ma20'])
        stock[code] = data

    return stock

def calc_vol_rate(rate = 2.0):
    news = readNews()
    bb = readBillboard()
    stock = readDataLastday()

    current = dc.get_today_all_multi()
    selected = []
    for j in range(len(current)):
        key = str(current.ix[j]['code'])
        if(key is None):
            continue
        try:
            row = current.ix[j]
            now = datetime.now()
            vr = float(row['volume']) * 60 * 4 / tradeTime(now.time()) / stock[key]['v5'] / 100 
            price = float(row['trade'])
            if ( vr > rate and price > 0 and (row['nmc'] / row['trade']) <= 800000   ##量比 > 2 ;流通盘小于80亿
                 and row['changepercent'] > 2.0 and row['changepercent'] < 7.0):     ##涨幅 2%到7%
                sel = {'0_date': now.date().isoformat(),
                       '1_code':key,
                       '2_name':row['name'],
                       '3_cp':row['changepercent'],
                       '4_price':row['trade'],
                       '5_vol_rate': '%.2f%%' % vr }
                ct._write_msg(" \n%s %s: %s" % (sel['1_code'],sel['2_name'],sel['5_vol_rate']))
                
                ##更多条件
                if( price <= stock[key]['ma10'] * 1.05                         ##当日开盘价在前日MA10的5%以内
                    and stock[key]['volume'] < stock[key]['v10'] * 1.5          ##前一天没有放巨量
                   ):
                    sel['7_note'] = '精选'
                    ct._write_msg(" <==精选")

                if( key in news):
                    sel['6_news'] = '利好'
                    ct._write_msg(" <==利好")

                if( key in bb.keys() ):
                    bbRow = bb[key]
                    sel['8_BB'] = '龙虎榜'
                    sel['8_1_count'] = bbRow['count_5']+'/'+bbRow['count_10']
                    sel['8_2_net'] = bbRow['net_5']+'/'+bbRow['net_10'] 
                    ct._write_msg(" <==龙虎榜：" + sel['8_1_count'] ) 

                selected.append(sel)
                

        except KeyError as e:
            continue

    path = ct.CSV_DIR + datetime.now().strftime('results/vr_open/%Y%m%d_%H%M/')
    os.mkdir(path)   
    pd.DataFrame(selected, dtype='str').to_csv(path+'select_vr.csv', encoding='utf8')
    return selected

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