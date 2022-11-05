import os
import math
import pandas as pd
import numpy as np
from tqdm import tqdm

input_amihud_fmt = './processed/{}/amihud.csv'
input_finance_fmt = './processed/{}/finance.csv'
input_hls_fmt = './processed/{}/hls.csv'
input_market_cap_fmt = './processed/{}/market_cap_quarterly.csv'
input_ret_fmt = './processed/{}/ret_quarterly.csv'
input_shares_est_fmt = './processed/{}/shares_est.csv'
input_stdv_fmt = './processed/{}/stdv.csv'
input_trend_fmt = './processed/{}/trend.csv'

output_file = './processed/aggregated.csv'

input_wui = pd.read_csv('./rawdata/wui.csv').dropna()
input_wui = input_wui.drop([input_wui.columns[1]], axis=1)
input_wuivnm = pd.read_csv('./processed/wui_vnm.csv').dropna()
input_vni = pd.read_csv('./processed/vni.csv').dropna()
input_vni = input_vni.drop([input_vni.columns[0]], axis=1)

crisis_quarter = set(['2007Q3','2007Q4','2008Q1','2008Q2','2008Q3','2008Q4','2009Q1','2009Q1','2020Q2','2020Q3','2020Q4','2021Q1','2021Q2','2021Q3','2021Q4'])

def get_next_quarter(quarter):
    y_str, q_str = quarter.split('Q')
    y, q = int(y_str), int(q_str)
    if q < 4:
        return '{}Q{}'.format(y, q+1)
    return '{}Q1'.format(y+1)

def process(code: str):
    print(code)
    input_amihud = pd.read_csv(input_amihud_fmt.format(code))
    input_amihud = input_amihud.drop(input_amihud.columns[0], axis=1)
    # print(input_amihud)
    input_finance = pd.read_csv(input_finance_fmt.format(code))
    input_finance = input_finance.drop(input_finance.columns[0], axis=1)
    # print(input_finance)
    input_hls = pd.read_csv(input_hls_fmt.format(code)).dropna()
    # print(input_hls)
    input_market_cap = pd.read_csv(input_market_cap_fmt.format(code)).dropna()
    input_market_cap = input_market_cap.drop([input_market_cap.columns[0], input_market_cap.columns[1], input_market_cap.columns[3]], axis=1).dropna()
    # print(input_market_cap)
    input_ret = pd.read_csv(input_ret_fmt.format(code)).dropna()
    input_ret = input_ret.drop([input_ret.columns[0]], axis=1).dropna()
    # print(input_ret)
    input_shares_est = pd.read_csv(input_shares_est_fmt.format(code))
    input_shares_est = pd.DataFrame({'quarter': input_shares_est['quarter'], 'lev': input_shares_est['lev']*100}).dropna()
    # print(input_shares_est)
    input_stdv = pd.read_csv(input_stdv_fmt.format(code)).dropna()
    # print(input_stdv)
    input_trend = pd.read_csv(input_trend_fmt.format(code)).dropna()
    # print(input_trend)

    output = input_amihud
    for table in [input_hls, input_finance, input_market_cap, input_ret, input_shares_est, input_stdv, input_trend, input_wui, input_wuivnm, input_vni]:
        output = pd.merge(output, table, on=['quarter'], how='inner')

    quarter = output['quarter']
    first_q = quarter[0]
    next_q = get_next_quarter(first_q)
    for q in quarter[1:]:
        if q != next_q:
            print(output)
            raise Exception('data not continuous, missing {}'.format(next_q))
        else:
            next_q = get_next_quarter(q)
    code_column = [code] * len(output)
    crisis_column = quarter.apply(lambda x: 1 if x in crisis_quarter else 0)
    output.insert(0, "code", code_column)
    output['crisis'] = crisis_column
    return output

def main():
    codes = None
    with open('codes.txt', 'r') as fd:
        codes = fd.readlines()
    codes = [c[:3] for c in codes]

    output_tables = list()
    for code in tqdm(codes):
        output_tables.append(process(code))
    output = pd.concat(output_tables)
    output = output.reset_index()
    output = output.drop([output.columns[0],output.columns[10],output.columns[11],output.columns[21],output.columns[22]], axis=1)
    output = output.rename({
        'amihud': 'illiq',
        'ln_amihud': 'ln_illiq',
        's': 'hsl',
        'ln_s': 'ln_hsl',
        'price_diff_perc': 'ret',
        'change_perc_mean': 'meanv',
        'change_perc_stdv': 'stdv',
        'WUI': 'wui',
        'market_cap': 'ln_size',
        'trend': 'gsv'
    }, axis=1)
    # print(output)
    # print(output.columns)
    output.to_csv(output_file)

if __name__ == '__main__':
    main()
    # process('SJF')
