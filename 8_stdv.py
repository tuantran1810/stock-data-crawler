import os
import math
import pandas as pd
import numpy as np
from tqdm import tqdm

input_price_fmt = './rawdata/{}/price_daily.csv'
output_stdv_quarterly_fmt = './processed/{}/stdv.csv'
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
    output_file = output_stdv_quarterly_fmt.format(code)

    os.makedirs(folder, exist_ok=True)
    if os.path.isfile(output_file):
        return

    price = pd.read_csv(input_price)
    price = price.drop([0, 1], axis=0)
    date = price['Attributes']
    quarter = date.apply(date_string_to_quarter)
    change_perc = price['change_perc2'].astype(float)
    tmp = pd.concat({'quarter': quarter, 'change_perc': change_perc}, axis=1)
    tmp.replace([np.inf, -np.inf], np.nan, inplace=True)
    tmp.dropna(inplace=True)
    change_perc_mean = tmp.groupby('quarter').mean()
    change_perc_mean = change_perc_mean.rename(columns={'change_perc': 'change_perc_mean'})
    change_perc_stdv = tmp.groupby('quarter').std()
    change_perc_stdv = change_perc_stdv.rename(columns={'change_perc': 'change_perc_stdv'})
    table = pd.merge(change_perc_mean, change_perc_stdv, on=['quarter'], how='inner')

    table.to_csv(output_file)

def main():
    codes = None
    with open('codes.txt', 'r') as fd:
        codes = fd.readlines()
    codes = [c[:3] for c in codes]

    for code in tqdm(codes):
        process(code)

if __name__ == '__main__':
    main()
    # process('SJF')
