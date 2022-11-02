import os
import math
import pandas as pd
import numpy as np
from tqdm import tqdm

input_price_fmt = './rawdata/{}/price_daily.csv'
output_liquidity_quarterly_fmt = './processed/{}/amihud.csv'
output_folder_fmt = './processed/{}'

def date_string_to_quarter(date_str: str):
    y_str, m_str, _ = date_str.split('-')
    y = int(y_str)
    m = int(m_str)
    q = int((m+2)/3)
    return '{}Q{}'.format(y, q)

def get_next_quarter(quarter):
    y_str, q_str = quarter.split('Q')
    y, q = int(y_str), int(q_str)
    if q < 4:
        return '{}Q{}'.format(y, q+1)
    return '{}Q1'.format(y+1)

def process(code: str):
    print(code)
    input_price = input_price_fmt.format(code)
    folder = output_folder_fmt.format(code)
    output_file = output_liquidity_quarterly_fmt.format(code)

    # os.makedirs(folder, exist_ok=True)
    # if os.path.isfile(output_file):
    #     return

    price = pd.read_csv(input_price)
    price = price.drop([0, 1], axis=0)
    date = price['Attributes']
    change_perc = price['change_perc2'].astype(float)
    value_match = price['value_match'].astype(float)
    value_reconcile = price['value_reconcile'].astype(float)

    value = value_match + value_reconcile

    change_abs = np.abs(change_perc)
    quarter = date.apply(date_string_to_quarter)
    tmp = pd.concat({'quarter': quarter, 'value': value, 'change_abs': change_abs}, axis=1)
    tmp = tmp[tmp['value'] > 0]
    tmp['amihud'] = 10e6*(tmp['change_abs'] + 10e-12)/tmp['value']
    tmp = tmp.drop(['value', 'change_abs'], axis=1)
    amihud_quarterly = tmp.groupby('quarter').mean().reset_index()
    amihud_quarterly['ln_amihud'] = np.log(amihud_quarterly['amihud'])

    quarter = amihud_quarterly['quarter'].to_list()
    last_q = quarter[-1]

    all_quarters = [quarter[0]]
    while all_quarters[-1] != last_q:
        q = all_quarters[-1]
        nq = get_next_quarter(q)
        all_quarters.append(nq)

    tmp = pd.DataFrame({'quarter': all_quarters})
    print(tmp)
    amihud_quarterly = pd.merge(tmp, amihud_quarterly, on=['quarter'], how='left')
    # amihud_quarterly.to_csv(output_file)
    print(amihud_quarterly)

def main():
    codes = None
    with open('codes.txt', 'r') as fd:
        codes = fd.readlines()
    codes = [c[:3] for c in codes]

    for code in tqdm(codes):
        process(code)

if __name__ == '__main__':
    # main()
    process('SJF')
