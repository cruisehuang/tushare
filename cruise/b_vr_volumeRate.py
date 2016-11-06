# -*- coding:utf-8 -*- 
'''
Created on 2016/10/10
Calculating Volume Rate
@author: Cruise Huang

'''
import os
from functools import partial
from multiprocessing.pool import Pool

import pandas as pd

# from tushare.stock import trading as td
# from tushare.stock import cons as ct

import config as cfg
import utils
import a_dc_dataCollect as dc


#Not used
'''
def get_tick_multi(symbols=None, date=None):
    ct._write_head()
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
'''
    

def calc_vol_rate(rate = 2.0):
    news = utils.readNews()
    strategy = utils.readStrategy()
    bb = utils.readBillboard()
    stock = utils.readDataLastday()

    current = dc.get_today_all_multi()
    selected = []
    for i,row in current.iterrows():
        key = str(row['code'])
        if(key is None):
            continue
        try:
            now = utils.now()
            vr = float(row['volume']) * 60 * 4 / utils.tradeTime(now.time()) / stock[key]['v5'] / 100 
            price = float(row['trade'])
            if ( vr > rate and price > 0 and (row['nmc'] / row['trade']) <= 800000   ##量比 > 2 ;流通盘小于80亿
                 and row['changepercent'] > 2.0 and row['changepercent'] < 7.0):     ##涨幅 2%到7%
                sel = {'0_date': now.date().isoformat(),
                       '1_code':key,
                       '2_name':row['name'],
                       '3_cp':row['changepercent'],
                       '4_price':row['trade'],
                       '5_vol_rate': '%.2f%%' % vr }
                utils.msg(" \n%s %s: %s" % (sel['1_code'],sel['2_name'],sel['5_vol_rate']))
                
                ##更多条件
                if( price <= stock[key]['ma10'] * 1.05                         ##当日开盘价在前日MA10的5%以内
                    and stock[key]['volume'] < stock[key]['v10'] * 1.5          ##前一天没有放巨量
                   ):
                    sel['7_note'] = '精选'
                    utils.msg(" <==精选")

                if( key in news.keys()):
                    sel['6_news'] = news[key]
                    utils.msg(" <==" + news[key])

                if( key in strategy.keys()):
                    sel['7_strategy'] = strategy[key]
                    utils.msg(" <==" + strategy[key])

                if( key in bb.keys() ):
                    bbRow = bb[key]
                    sel['8_BB'] = '龙虎榜'
                    sel['8_1_count'] = str(bbRow['count_5'])+'/'+str(bbRow['count_10'])
                    sel['8_2_net'] = str(bbRow['net_5'])+'/'+str(bbRow['net_10']) 
                    utils.msg(" <==龙虎榜：" + sel['8_1_count'] ) 

                selected.append(sel)
                

        except KeyError as e:
            continue

    path = cfg.PATH_2_RESULTS + utils.now().strftime('vr_open/%Y%m%d_%H%M/')
    os.mkdir(path)   
    pd.DataFrame(selected, dtype='str').to_csv(path+'select_vr.csv', encoding='utf8')
    return selected

def main():
    if(utils.pathExists(cfg.FILE_LAST_HIS) == False):
        print('Yesterday His File Not existed!')
        return

    utils.getPath(cfg.PATH_2_RESULTS)

    calc_vol_rate()
 
if __name__ == '__main__':
    main()