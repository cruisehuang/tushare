# -*- coding:utf-8 -*- 
'''
Created on 2016/10/07
@author: Cruise Huang
'''
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
    p = Pool(8)
    results = p.map(multiFunc, range(1,ct.PAGE_NUM[0]))
#    p.close()
#    p.join()

    df = pd.DataFrame()
    for result in results:
        df=df.append(result, ignore_index=True);

    print('\n'+du.get_now()+' 获取当日全部数据结束')
    return df

def get_hists_multi(symbols, start=None, end=None,
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
        p = Pool(16)
        results = p.map(multiFunc, symbols)
#        p.close()
#        p.join()

        df = pd.DataFrame()
        for result in results:
            df=df.append(result, ignore_index=True);

        print('\n'+du.get_now()+' 批量获取历史行情数据结束')
        return df
    else:
        return None

def get_all_his_2_csv():
    stockCodes = get_today_all_multi()
    stockCodes.to_csv(ct.CSV_DIR+'codes.csv', columns=['code','name'])

    codes = stockCodes['code']
    date = du.last_tddate()
    df = get_hists_multi(codes,date,date)
  
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
            if ( (float(current.ix[j]['volume']) / v5[key] / 100 ) > rate):
                select.append(key)
        except KeyError as e:
            print('Code not found:' + key)
            continue

    pd.DataFrame(select, dtype='str').to_csv(ct.CSV_DIR+'select_by_vol_rate.csv')
    return select

def main():
    get_all_his_2_csv();
  
 
if __name__ == '__main__':
    main()