import os
import math
import pandas as pd
import numpy as np
from tqdm import tqdm

input_fmt = './rawdata/{}/finance.csv'
output_fmt = './processed/{}/finance.csv'
output_folder_fmt = './processed/{}'

def process(code: str):
    print(code)
    inp = input_fmt.format(code)
    folder = output_folder_fmt.format(code)
    output_file = output_fmt.format(code)

    os.makedirs(folder, exist_ok=True)
    if os.path.isfile(output_file):
        return

    inp = pd.read_csv(inp)
    quarter = inp['Quý']
    roa = inp['ROAA(%)']
    tmp = pd.concat({'quarter': quarter, 'roa': roa}, axis=1)
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
