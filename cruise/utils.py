# -*- coding:utf-8 -*- 
'''
Created on 2016/11/05
Utils, file io, datetime, ...
@author: Cruise Huang
'''

import sys
import os.path
import shutil
from datetime import datetime,date,time,timedelta

import pandas as pd

import config as cfg

def pathExists(path):
    return os.path.exists(path)

def getPath(path):
    if( pathExists(path) == False ):
        os.mkdir(path) 
    return path;

def now():
    return datetime.today();

def nowStr():
    return now().strftime('%Y-%m-%d %H:%M:%S')

def curDateStr(strFormat):
    return now().date().strftime(strFormat)

def curTimeStr(strFormat):
    return now().time().strftime(strFormat)

def msg(msg):
    sys.stdout.write(msg)
    sys.stdout.flush()


def isHoliday(date):
    df = pd.read_csv(cfg.FILE_ALL_CALENDAR)

    holiday = df[df.isOpen == 0]['calendarDate'].values
    if isinstance(date, str):
        today = datetime.strptime(date, '%Y-%m-%d')
    if today.isoweekday() in [6, 7] or date.replace('/0','/') in holiday:
        return True
    else:
        return False

def copy(src, dst):
    shutil.copy(src, dst)


def timeDiff(t1,t2):
    d = now().date()
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
    path2News = cfg.PATH_2_NEWS + curDateStr('%Y%m%d') +'.csv'
    if(pathExists(path2News) == False):
        print(path2News + 'Not existed!')
        return []

    news = pd.read_csv(path2News, dtype='str', encoding='utf8')
    codesInNews = dict()
    for i,r in news.iterrows():
        code = r['code'].split('.')[0]
        codesInNews[code] = r['remark']     

    return codesInNews

def readStrategy():
    path2Strategy = cfg.PATH_2_STRATEGY

    rumors = pd.read_csv(path2Strategy+'focus.csv', dtype='str', encoding='utf8')
    holding = pd.read_csv(path2Strategy+'holding.csv', dtype='str', encoding='utf8')

    total = rumors.append(holding,ignore_index=True)

    codesInStrategy = dict()
    for i,r in total.iterrows():
        code = r['code'].split('.')[0]
        codesInStrategy[code] = r['remark']     

    return codesInStrategy

def readBillboard():
    path2BB= cfg.PATH_2_BILLBOARD + curDateStr('%Y%m%d') +'_merged.csv'
    if(os.path.exists(path2BB) == False):
        print(path2BB + 'Not existed!')
        return []

    bb = pd.read_csv(path2BB, dtype='str', encoding='utf8')
    bbDict = dict()
    for i,r in bb.iterrows():
        code = r['code']
        bbDict[code] = r

    return bbDict

def readDataLastday():
    loaded = pd.read_csv(cfg.FILE_LAST_HIS, dtype='str', encoding='utf8')
    stock = dict()

    for i,r in loaded.iterrows():
        code = r['code']
        data = dict()
        #data['price'] = float(loaded.ix[i]['close'])
        data['volume'] = float(r['volume'])
        #data['per'] = float(loaded.ix[i]['p_change'])
        #data['ma5'] = float(loaded.ix[i]['ma5'])
        data['ma10'] = float(r['ma10'])
        #data['ma20'] = float(loaded.ix[i]['ma20']) 
        data['v5'] = float(r['v_ma5'])
        data['v10'] = float(r['v_ma10'])
        #data['v20'] = float(loaded.ix[i]['v_ma20'])
        stock[code] = data

    return stock


