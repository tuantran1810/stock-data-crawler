import os
import math
import pandas as pd
import numpy as np
from tqdm import tqdm

def date_string_to_quarter1(date_str: str):
    y_str, m_str, _ = date_str.split('-')
    y = int(y_str)
    m = int(m_str)
    q = int((m+2)/3)
    return '{}Q{}'.format(y, q)

def date_string_to_quarter2(date_str: str):
    _, m_str, y_str = date_str.split('-')
    y = int(y_str)
    m = int(m_str)
    q = int((m+2)/3)
    return '{}Q{}'.format(y, q)

def process_wuivnm():
    inp = pd.read_csv('./rawdata/wui_vnm.csv')
    date = inp['observation_date']
    wuivnm = inp['WUIVNM'].astype(float)
    quarter = date.apply(date_string_to_quarter1)
    tmp = pd.concat({'quarter': quarter, 'wuivnm': wuivnm}, axis=1)
    tmp = tmp.groupby('quarter').sum()
    tmp.to_csv('./processed/wui_vnm.csv')

def process_vni():
    inp = pd.read_csv('./rawdata/vni.csv')
    date = inp['date']
    vni = inp['vni'].astype(float)
    quarter = date.apply(date_string_to_quarter2)
    tmp = pd.concat({'quarter': quarter, 'vni': vni}, axis=1)
    tmp.to_csv('./processed/vni.csv')

if __name__ == '__main__':
    process_wuivnm()
    process_vni()
