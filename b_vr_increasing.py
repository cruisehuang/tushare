# -*- coding:utf-8 -*- 
'''
Created on 2016/10/10
Calculating continuously VR increase
@author: Cruise Huang

'''
import os,os.path
from datetime import datetime,date,time,timedelta
import time as tm
import cmath
from functools import partial
from multiprocessing.pool import Pool

import pandas as pd

from tushare.stock import trading as td
from tushare.stock import cons as ct
from tushare.util import dateu as du

import a_dc_dataCollect as dc
import b_vr_volumeRate as vr

INTERVAL = 0.1 # minutes

def fillinVr(lastDay, time, currentData):
    for i,r in currentData.iterrows():
        key = r['code']
        volumeRate = float(r['volume']) * 60 * 4 / vr.tradeTime(time) / lastDay[key]['v5'] / 100
        currentData['vr'] = volumeRate



def calcu(current, last):
    selected = []
    bb = vr.readBillboard()
    merged = last.merge(current, on='code',suffixes=('','_cur'))

    for i,r in merged.iterrows():
        if(cmath.isclose(float(r['trade']),0.0)):
            continue

        key = r['code']
        priceRate = (float(r['trade_cur']) - float(r['trade'])) / float(r['trade'])
        vrRate = (float(r['vr_cur']) - float(r['vr'])) / float(r['vr'])
        if(priceRate > 0 and vrRate > 0): 
            sel = {
                   '1_code':key,
                   '2_name':r['name'],
                   '3_p_rate': '%.2f%%' % priceRate,
                   '4_vr_rate': '%.2f%%' % vrRate
                   }
            ct._write_msg(" \n%s %s: 价格上涨%s 量比增加%s" % (sel['1_code'],sel['2_name'],sel['3_p_rate'],sel['4_vr_rate']))

            if( key in bb.keys() ):
                bbRow = bb[key]
                sel['8_BB'] = '龙虎榜'
                sel['8_1_count'] = str(bbRow['count_5'])+'/'+str(bbRow['count_10'])
                sel['8_2_net'] = str(bbRow['net_5'])+'/'+str(bbRow['net_10']) 
                ct._write_msg(" <==龙虎榜：" + sel['8_1_count'] ) 

            selected.append(sel)

    now = datetime.now()
    path = ct.CSV_DIR + now.strftime('results/vr_cont/%Y%m%d/')
    if(os.path.exists(path) == False):
        os.mkdir(path)
    pd.DataFrame(selected, dtype='str').to_csv(path+now.strftime('%H%M.csv'),encoding='gbk')


def main():
    path2Ref = ct.CSV_DIR+'stocks_his_lastday.csv'
    if(os.path.exists(path2Ref) == False):
        print('Ref File Not existed!')
        return

    resultPath = ct.CSV_DIR+'results/'
    if(os.path.exists(resultPath) == False):
        os.mkdir(resultPath)

    
    todayAll = dict()
    dataLastday = vr.readDataLastday()
    last = None
    while True:
        now = datetime.now().time()
        ct._write_msg('\r'+now.isoformat())

        '''
        if(now > time(hour=11,minute=30) and now < time(hour=13)):
            continue
        if(now < time(hour=9, minute=31) or now > time(hour=15)):
            break
        '''
        ct._write_msg('\n')
        current = dc.get_today_all_multi()
        fillinVr(lastDay = dataLastday, time=now, currentData=current)

        todayAll[now.isoformat()] = current #not used for now

        if(last is not None):
            calcu(current=current, last=last)
        
        last = current

        tm.sleep(60*INTERVAL)
  
 
if __name__ == '__main__':
    main()