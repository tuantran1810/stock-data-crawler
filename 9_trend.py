import os
import math
import pandas as pd
import numpy as np
from tqdm import tqdm

input_fmt = './rawdata/{}/trend.csv'
output_fmt = './processed/{}/trend.csv'
output_folder_fmt = './processed/{}'

def date_string_to_quarter(date_str: str):
    y_str, m_str, _ = date_str.split('-')
    y = int(y_str)
    m = int(m_str)
    q = int((m+2)/3)
    return '{}Q{}'.format(y, q)

def process(code: str):
    print(code)
    inp = input_fmt.format(code)
    folder = output_folder_fmt.format(code)
    output_file = output_fmt.format(code)

    os.makedirs(folder, exist_ok=True)
    if os.path.isfile(output_file):
        return

    inp = pd.read_csv(inp)
    date = inp['date']
    trend = inp[code].astype(float)
    quarter = date.apply(date_string_to_quarter)
    tmp = pd.concat({'quarter': quarter, 'trend': trend}, axis=1)
    tmp = tmp.groupby('quarter').sum()
    tmp['trend'] = 100.0*tmp['trend']/tmp['trend'].max()
    tmp.to_csv(output_file)

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
