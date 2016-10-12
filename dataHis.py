# -*- coding:utf-8 -*- 
'''
Created on 2016/10/07
Data Collecting
@author: Cruise Huang
'''
import os.path
from datetime import datetime,date,time,timedelta
from functools import partial
from multiprocessing.pool import Pool

import pandas as pd

from tushare.stock import trading as td
from tushare.stock import cons as ct
from tushare.util import dateu as du


def get_today_all_multi():
    ct._write_head()
    print(du.get_now()+' 获取当日全部数据')

    multiFunc = partial(td._parsing_dayprice_json)
    with Pool(12) as p:
        results = p.map(multiFunc, range(1,ct.PAGE_NUM[0]))
        p.close()
        p.join()

    df = pd.DataFrame()
    for result in results:
        df=df.append(result, ignore_index=True);

    print('\n'+du.get_now()+' 获取当日全部数据结束')
    return df

def write_his(result, path):
    ct._write_msg('\rWriting to:' + path)
    result.to_csv(path)

def get_his(symbol, write2disk):
    path2Stock = ct.CSV_DIR+'historyData/'+ symbol +'.csv'
    path2Codes = ct.CSV_DIR+'codes.csv'
    if(os.path.exists(path2Stock) and 
       datetime.fromtimestamp(os.path.getmtime(path2Codes)).date() == datetime.fromtimestamp(os.path.getmtime(path2Stock)).date()):
        return

    df = td.get_hist_data(code=symbol,start=None, end=None,
                          ktype='D', retry_count=3, pause=0.001)
    if(write2disk and df is not None):
        write_his(df, path2Stock)


def get_hists_multi(symbols, write2disk=False, start=None, end=None,
                    ktype='D', retry_count=3,
                    pause=0.001):
    """
    批量获取历史行情数据，具体参数和返回数据类型请参考get_hist_data接口
    """
    ct._write_head()
    print(du.get_now()+' 批量获取历史行情数据')
    if isinstance(symbols, list) or isinstance(symbols, set) or isinstance(symbols, tuple) or isinstance(symbols, pd.Series):
        multiFunc = partial(get_his, write2disk=True)
        with Pool(8) as p:
            results = p.map(multiFunc, symbols)
            p.close()
            p.join()

        print('\n'+du.get_now()+' 批量获取历史行情数据结束')
    else:
        return None


def write_stockcodes():
    stockCodes = get_today_all_multi()
    stockCodes.to_csv(ct.CSV_DIR+'all_today.csv')
    stockCodes.to_csv(ct.CSV_DIR+'codes.csv', columns=['code','name'])

def write_all_his():
    loaded = pd.read_csv(ct.CSV_DIR+'codes.csv', dtype='str',encoding='gbk')
    get_hists_multi(loaded['code'], write2disk=True)

def read_his(code):
    ct._write_msg('\rReading: '+code)
    df = pd.read_csv(ct.CSV_DIR+'historyData/'+ code+'.csv', dtype='str',encoding='gbk')
    return df

def read_all_his():
    loaded = pd.read_csv(ct.CSV_DIR+'codes.csv', dtype='str',encoding='gbk')
    allHis = pd.DataFrame()
    lastDay = pd.DataFrame()

    readFunc = partial(read_his)
    with Pool(16) as rp:
        results = rp.map(readFunc, loaded['code'])
        rp.close()
        rp.join()

    for stock in results:
        ct._write_msg('\rHandling: '+ stock.ix[0]['code'])
        allHis = allHis.append(stock,ignore_index=True)
        lastDay = lastDay.append(stock.head(1),ignore_index=True)

    return [allHis,lastDay]


def write_all_lastday(df):
    df.to_csv(ct.CSV_DIR+'stocks_his_lastday.csv')
    return df


def main():
    now = datetime.today()
    path2Codes = ct.CSV_DIR+'codes.csv'
    path2Stock = ct.CSV_DIR+'historyData/'

    fileCount =  len(os.listdir(path2Stock))
        
    if(os.path.exists(path2Codes) == False 
       or (now.time() > time(hour=15) and datetime.fromtimestamp(os.path.getmtime(path2Codes)).date() < now.date())
       or fileCount < len(pd.read_csv(ct.CSV_DIR+'codes.csv', dtype='str',encoding='gbk'))):
        write_stockcodes()
        write_all_his()
    else:
        df = read_all_his()
        write_all_lastday(df[1])
 
if __name__ == '__main__':
    main()