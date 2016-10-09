# -*- coding:utf-8 -*- 
'''
Created on 2016/10/07
@author: Cruise Huang
'''
from datetime import datetime,date,time
from functools import partial
from multiprocessing import Value,Lock
from multiprocessing.pool import Pool

import pandas as pd

from tushare.stock import trading as td
from tushare.stock import cons as ct
from tushare.util import dateu as du


def get_today_all_multi():
    ct._write_head()
    print(du.get_now()+' 获取当日全部数据')

    multiFunc = partial(td._parsing_dayprice_json)
    with Pool(8) as p:
        results = p.map(multiFunc, range(1,ct.PAGE_NUM[0]))
        p.close()
        p.join()

    df = pd.DataFrame()
    for result in results:
        df=df.append(result, ignore_index=True);

    print('\n'+du.get_now()+' 获取当日全部数据结束')
    return df

def write_his(result):
    path = ct.CSV_DIR+'historyData/'+result.ix[0]['code']+'.csv'
    ct._write_msg('\rWriting to:' + path)
    result.to_csv(path)

def get_hists_multi(symbols, write2disk=False, start=None, end=None,
                    ktype='D', retry_count=3,
                    pause=0.001):
    """
    批量获取历史行情数据，具体参数和返回数据类型请参考get_hist_data接口
    """
    ct._write_head()
    print(du.get_now()+' 批量获取历史行情数据')
    if isinstance(symbols, list) or isinstance(symbols, set) or isinstance(symbols, tuple) or isinstance(symbols, pd.Series):
        multiFunc = partial(td.get_hist_data,start=start, end=end,
                                             ktype=ktype, retry_count=retry_count,
                                             pause=pause)
        with Pool(16) as p:
            results = p.map(multiFunc, symbols)
            p.close()
            p.join()

        df = pd.DataFrame()
        for result in results:
            df=df.append(result, ignore_index=True);
            if(write2disk == True):
                write_his(result)

#        if(write2disk == True):
#            ct._write_msg('\n')
#            writeFunc = partial(write_his)
#            with Pool(8) as wp:
#                wp.map(writeFunc, results)
#                wp.close()
#                wp.join()

        print('\n'+du.get_now()+' 批量获取历史行情数据结束')
        return df
    else:
        return None

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
    


def write_stockcodes():
    stockCodes = get_today_all_multi()
    stockCodes.to_csv(ct.CSV_DIR+'all_today.csv')
    stockCodes.to_csv(ct.CSV_DIR+'codes.csv', columns=['code','name'])

def write_all_his():
    loaded = pd.read_csv(ct.CSV_DIR+'codes.csv', dtype='str',encoding='gbk')
    get_hists_multi(loaded['code'], write2disk=True)

def get_all_lastday():
    loaded = pd.read_csv(ct.CSV_DIR+'codes.csv', dtype='str',encoding='gbk')
    codes = loaded['code']
    date = du.last_tddate()

    df = get_hists_multi(codes, start=date, end=date)
  
    df.to_csv(ct.CSV_DIR+'stocks_his_lastday.csv')
    df.to_csv(ct.CSV_DIR+'stocks_v5_lastday.csv', columns=['code','v_ma5'])
    return df

def calc_vol_rate(rate = 2.0):
    loaded = pd.read_csv(ct.CSV_DIR+'stocks_v5_lastday.csv', dtype='str')
    
    v5 = dict();
    for i in range(len(loaded)):
        code = loaded.ix[i]['code']
        v5[code] = float(loaded.ix[i]['v_ma5'])

    current = get_today_all_multi()
    select = []
    for j in range(len(current)):
        key = str(current.ix[j]['code'])
        if(key is None):
            continue
        try:
            row = current.ix[j]
            #The first minute
            if ( (float(row['volume']) * 60 * 4 / v5[key] / 100 ) > rate
                 and row['changepercent'] > 2.0
                 and row['trade'] > 0.0 and (row['nmc'] / row['trade']) <= 30000):
                ct._write_msg(key + '\t')
                data = {'code':key,
                        'name':row['name'],
                        'cp':row['changepercent'],
                        'price':row['trade']}
                select.append(data)
        except KeyError as e:
            continue

    pd.DataFrame(select, dtype='str').to_csv(ct.CSV_DIR+'select_by_vol_rate.csv')
    return select

def main():
    now = datetime.today()
    if(du.is_holiday(now.date().strftime('%Y/%m/%d'))
       or now.time()<time(hour=9,minute=30) or now.time()>time(hour=3)):
        write_stockcodes()
        write_all_his()
        get_all_lastday()
    else:
        calc_vol_rate()
  
 
if __name__ == '__main__':
    main()