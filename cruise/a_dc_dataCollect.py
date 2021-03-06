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
from tushare.stock import billboard as bb
from tushare.stock import cons as ct

import config as cfg
import utils


def get_today_all_multi():
    ct._write_head()
    print('获取当日全部数据')
    multiFunc = partial(td._parsing_dayprice_json)
    with Pool(12) as p:
        results = p.map(multiFunc, range(1,ct.PAGE_NUM[0]))
        p.close()
        p.join()

    df = pd.DataFrame()
    for result in results:
        df=df.append(result, ignore_index=True);

    print('\n获取当日全部数据结束')
    return df

def write_his(result, path):
    utils.msg('\rWriting to:' + path)
    result.to_csv(path, encoding='utf8')

def get_his(symbol, write2disk):
    path2Stock = cfg.PATH_2_HIS_DATA + symbol +'.csv'
    if(os.path.exists(path2Stock) and 
       datetime.fromtimestamp(os.path.getmtime(cfg.FILE_CODES)).date() == datetime.fromtimestamp(os.path.getmtime(path2Stock)).date()):
        return

    df = td.get_hist_data(code=symbol,start=None, end=None,
                          ktype='D', retry_count=3, pause=0.001)
    df.insert(loc=0,column='code',value=symbol)

    if(write2disk and df is not None):
        write_his(df, path2Stock)


def get_hists_multi(symbols, write2disk=False, start=None, end=None,
                    ktype='D', retry_count=1,
                    pause=0.01):
    """
    批量获取历史行情数据，具体参数和返回数据类型请参考get_hist_data接口
    """
    ct._write_head()
    print('批量获取历史行情数据')
    if isinstance(symbols, list) or isinstance(symbols, set) or isinstance(symbols, tuple) or isinstance(symbols, pd.Series):
        multiFunc = partial(get_his, write2disk=True)
        with Pool(8) as p:
            results = p.map(multiFunc, symbols)
            p.close()
            p.join()

        print('\n批量获取历史行情数据结束')
    else:
        return None


def write_stockcodes():
    stockCodes = get_today_all_multi()
    stockCodes.to_csv(cfg.MAIN_DIR+'all_today.csv',encoding='utf8')
    stockCodes.to_csv(cfg.FILE_CODES, columns=['code','name'], encoding='utf8')

def write_all_his():
    loaded = pd.read_csv(cfg.FILE_CODES, dtype='str', encoding='utf8')
    get_hists_multi(loaded['code'], write2disk=True)

def read_his(code):
    utils.msg('\rReading: '+code)

    codeHisPath = cfg.PATH_2_HIS_DATA + code+'.csv'

    if(os.path.exists(codeHisPath) == False):
        print('Path Not Exists:' + code)
        return None

    df = pd.read_csv(codeHisPath, dtype='str', encoding='utf8')
    return df

def read_all_his():
    loaded = pd.read_csv(cfg.FILE_CODES, dtype='str', encoding='utf8')
    allHis = pd.DataFrame()
    lastDay = pd.DataFrame()

    readFunc = partial(read_his)
    with Pool(16) as rp:
        results = rp.map(readFunc, loaded['code'])
        rp.close()
        rp.join()

    for stock in results:
        if(stock is None):
            continue
        utils.msg('\rHandling: '+ stock.ix[0]['code'])
        allHis = allHis.append(stock,ignore_index=True)
        lastDay = lastDay.append(stock.head(1),ignore_index=True)

    return [allHis,lastDay]


def write_all_lastday(df):
    df.to_csv(cfg.MAIN_DIR+'stocks_his_lastday.csv', encoding='utf8')
    return df

def write_billboard():
    print('获取龙虎榜数据')
    path = utils.getPath(cfg.PATH_2_BILLBOARD)

    dateStr = (utils.now() + timedelta(days=1)).strftime('%Y%m%d')

    df5 = bb.cap_tops(days=5)
    #df5.to_csv(PATH_2_BILLBOARD+dateStr+'_5d.csv',encoding='utf8')
    df10 = bb.cap_tops(days=10)
    #df10.to_csv(PATH_2_BILLBOARD+dateStr+'_10d.csv',encoding='utf8')
    print('\n获取龙虎榜数据结束')

    merged = df5.merge(df10, on=['code','name'],suffixes=('_5','_10'),how='outer')
    merged.to_csv(path+dateStr+'_merged.csv', encoding='utf8')


def main():
    now = utils.now()
    fileCount =  len(os.listdir(cfg.PATH_2_HIS_DATA))
        
    if(os.path.exists(cfg.FILE_CODES) == False 
       or (now.time() > time(hour=15) and datetime.fromtimestamp(os.path.getmtime(cfg.FILE_CODES)).date() < now.date())
       or fileCount < len(pd.read_csv(cfg.FILE_CODES, dtype='str', encoding='utf8'))):
        write_stockcodes()
        write_all_his()
    
    df = read_all_his()
    write_all_lastday(df[1])

    write_billboard()
 
if __name__ == '__main__':
    main()