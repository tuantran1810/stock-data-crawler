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

def process(code: str):
    print(code)
    input_price = input_price_fmt.format(code)
    folder = output_folder_fmt.format(code)
    output_file = output_liquidity_quarterly_fmt.format(code)

    os.makedirs(folder, exist_ok=True)
    if os.path.isfile(output_file):
        return

    price = pd.read_csv(input_price)
    price = price.drop([0, 1], axis=0)
    date = price['Attributes']
    change_perc = price['change_perc2'].astype(float)
    value_match = price['value_match'].astype(float)
    value_reconcile = price['value_reconcile'].astype(float)

    value = value_match + value_reconcile + 1

    change_abs = np.abs(change_perc)
    tmp = 10e6 * change_abs/value
    quarter = date.apply(date_string_to_quarter)
    tmp = pd.concat({'quarter': quarter, 'amihud': tmp}, axis=1)
    amihud_quarterly = tmp.groupby('quarter').mean()
    amihud_quarterly.to_csv(output_file)

def main():
    codes = None
    with open('codes.txt', 'r') as fd:
        codes = fd.readlines()
    codes = [c[:3] for c in codes]

    for code in tqdm(codes):
        process(code)

if __name__ == '__main__':
    # main()
    process('DGW')
