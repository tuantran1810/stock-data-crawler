import os
import math
import pandas as pd
import numpy as np
from tqdm import tqdm

input_amihud_fmt = './processed/{}/amihud.csv'
input_hls_fmt = './processed/{}/hls.csv'
input_market_cap_fmt = './processed/{}/market_cap_quarterly.csv'
input_stdv_fmt = './processed/{}/stdv.csv'
output_file = './processed/aggregated.csv'

def get_next_quarter(quarter):
    y_str, q_str = quarter.split('Q')
    y, q = int(y_str), int(q_str)
    if q < 4:
        return '{}Q{}'.format(y, q+1)
    return '{}Q1'.format(y+1)

def process(code: str):
    print(code)
    input_amihud = pd.read_csv(input_amihud_fmt.format(code))
    input_hls = pd.read_csv(input_hls_fmt.format(code)).dropna()
    input_market_cap = pd.read_csv(input_market_cap_fmt.format(code)).dropna()
    input_market_cap = input_market_cap.drop([input_market_cap.columns[0], input_market_cap.columns[1], input_market_cap.columns[3]], axis=1).dropna()
    input_stdv = pd.read_csv(input_stdv_fmt.format(code)).dropna()

    output = input_amihud
    for table in [input_hls, input_market_cap, input_stdv]:
        output = pd.merge(output, table, on=['quarter'], how='inner')

    quarter = output['quarter']
    first_q = quarter[0]
    next_q = get_next_quarter(first_q)
    for q in quarter[1:]:
        if q != next_q:
            raise Exception('data not continuous, missing {}'.format(next_q))
        else:
            next_q = get_next_quarter(q)
    code_column = [code] * len(output)
    output.insert(0, "code", code_column)
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
    output.to_csv(output_file)

if __name__ == '__main__':
    main()
    # process('SJF')
