# -*- coding:utf-8 -*- 
'''
Created on 2016/10/10
Calculating continuously VR increase
@author: Cruise Huang

'''
from datetime import time
import time as tm
import cmath
from functools import partial
from multiprocessing.pool import Pool

import pandas as pd

import config as cfg
import utils
import a_dc_dataCollect as dc


INTERVAL = 1.5 # minutes
PATH_2_VR = cfg.PATH_2_RESULTS + 'vr_cont/'

def fillinVr(lastDay, time, currentData):
    for i,r in currentData.iterrows():
        key = r['code']
        if(key in lastDay.keys()):
            volumeRate = float(r['volume']) * 60 * 4 / utils.tradeTime(time) / lastDay[key]['v5'] / 100
            currentData.set_value(i,'vr',volumeRate)

def calcu(current, last,counting):
    selected = []
    bb = utils.readBillboard()
    news = utils.readNews()
    strategy = utils.readStrategy()
    merged = last.merge(current, on='code',suffixes=('','_cur'))

    alert = False
    for i,r in merged.iterrows():
        if(cmath.isclose(float(r['trade']),0.0) or cmath.isclose(float(r['vr']),0.0)):
            continue

        key = r['code']
        priceRate = (float(r['trade_cur']) - float(r['trade'])) / float(r['trade']) * 100 #%
        vrRate = (float(r['vr_cur']) - float(r['vr'])) / float(r['vr']) * 100 #%
        if(priceRate > 1.0 and vrRate > 1.0 and float(r['vr_cur']) > 2.0): 
            sel = {
                   '1_code':key,
                   '2_name':r['name'],
                   '2_1_per': '%.2f%%' % r['changepercent'],
                   '3_p_rate': '%.2f%%' % priceRate,
                   '4_vr_rate': '%.2f%%' % vrRate
                   }
            utils.msg(" \n%s %s : 涨幅%s 价格上涨%s 量比增加%s" 
                          % (sel['1_code'],sel['2_name'],sel['2_1_per'],sel['3_p_rate'],sel['4_vr_rate']))

            if( key not in counting.keys()):
                counting[key] = 1
            else:
                counting[key] += 1
                if(counting[key] >= 3):
                    alert = True
            utils.msg("  今日次数：" + str(counting[key]))

            if( key in news.keys()):
                sel['6_news'] = news[key]
                utils.msg(" <==" + news[key])

            if( key in strategy.keys()):
                sel['7_strategy'] = strategy[key]
                utils.msg(" <==" + strategy[key])
                alert = True

            if( key in bb.keys() ):
                bbRow = bb[key]
                sel['8_BB'] = '龙虎榜'
                sel['8_1_count'] = str(bbRow['count_5'])+'/'+str(bbRow['count_10'])
                sel['8_2_net'] = str(bbRow['net_5'])+'/'+str(bbRow['net_10']) 
                utils.msg(" <==龙虎榜：" + sel['8_1_count'] )

            selected.append(sel)

    if(len(selected) > 0):
        path = PATH_2_VR + utils.curDateStr('%Y%m%d/')
        utils.getPath(path)
        pd.DataFrame(selected, dtype='str').to_csv(path+utils.curTimeStr('%H%M.csv'), encoding='utf8')

    if(alert == True):
        utils.alert()

def main():
    if(utils.pathExists(cfg.FILE_LAST_HIS) == False):
        print('Yesterday His File Not existed!')
        return

    utils.getPath(cfg.PATH_2_RESULTS)

    
    todayAll = dict()
    dataLastday = utils.readDataLastday()
    selectedCount = dict() #{code,count}
    selectedPath = PATH_2_VR + utils.curDateStr('%Y%m%d/selectedToday111.csv')
    last = None

    while True:
        now = utils.now().time()
        
        if(now > time(hour=11,minute=30) and now < time(hour=13)):
            continue
        if(now < time(hour=9, minute=31) or now > time(hour=15)):
            break
        utils.msg(now.strftime('\n%H:%M\n'))
        
        try:
            current = dc.get_today_all_multi()
        except Exception as e:
            print(e)
            tm.sleep(10)
            continue

        fillinVr(lastDay = dataLastday, time=now, currentData=current)
        todayAll[now.strftime('%H%M')] = current #not used for now

        if(last is not None):
            calcu(current=current, last=last, counting=selectedCount)
        
        last = current

        if(len(selectedCount)>0):
            df = pd.DataFrame.from_dict(selectedCount, orient='index', dtype='str')
            df.index.names=['1_code']
            df.columns = ['count']
            df.to_csv(selectedPath, encoding='utf8')

        tm.sleep(60*INTERVAL)

    utils.copy(selectedPath, cfg.PATH_2_REVIEW+utils.curDateStr('%Y-%m-%d_cont.csv'))
 
if __name__ == '__main__':
    main()