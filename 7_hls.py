import os
import math
import pandas as pd
import numpy as np
from tqdm import tqdm

input_price_fmt = './rawdata/{}/price_daily.csv'
output_hls_quarterly_fmt = './processed/{}/hls.csv'
output_folder_fmt = './processed/{}'

alpha_const_1 = 3-2*(2**(1/2))
alpha_const_2 = (2**(1/2)) - 1

def date_string_to_quarter(date_str: str):
    y_str, m_str, _ = date_str.split('-')
    y = int(y_str)
    m = int(m_str)
    q = int((m+2)/3)
    return '{}Q{}'.format(y, q)

def calculate_alpha_s(beta, gamma):
    sqrt_beta = beta**(1/2)
    alpha = ((sqrt_beta*alpha_const_2)/alpha_const_1) - ((gamma / alpha_const_1)**(1/2))
    exp_alpha = np.exp(alpha)
    s = (2 * (exp_alpha - 1)) / (1 + exp_alpha)
    return alpha, s

def process(code: str):
    print(code)
    input_price = input_price_fmt.format(code)
    folder = output_folder_fmt.format(code)
    output_file = output_hls_quarterly_fmt.format(code)

    os.makedirs(folder, exist_ok=True)
    if os.path.isfile(output_file):
        return

    price = pd.read_csv(input_price)
    price = price.drop([0, 1], axis=0)
    date = price['Attributes']
    quarter = date.apply(date_string_to_quarter)
    d_high = price['high'].astype(float)
    d_low = price['low'].astype(float)
    d_1_high = price['high'].astype(float)
    d_1_low = price['low'].astype(float)

    table = pd.concat({'date': date,'quarter': quarter, 'd_high': d_high, 'd_low': d_low, 'd_1_high': d_1_high, 'd_1_low': d_1_low}, axis=1)
    table['d_1_high'] = table['d_1_high'].shift(-1)
    table['d_1_low'] = table['d_1_low'].shift(-1)
    table = table[:-1]

    d_0_delta = table['d_high'] - table['d_low']
    d_1_delta = table['d_1_high'] - table['d_1_low']
    d_0_ratio = table['d_high'] / table['d_low']
    d_1_ratio = table['d_1_high'] / table['d_1_low']
    beta = (d_1_delta)**2 + (d_0_delta)**2
    ln_beta = np.log(d_0_ratio)**2 + np.log(d_1_ratio)**2

    g_high = pd.concat({'d1': table['d_1_high'], 'd': table['d_high']}, axis=1)
    g_low = pd.concat({'d1': table['d_1_low'], 'd': table['d_low']}, axis=1)
    g_high['max'] = g_high.max(axis=1)
    g_low['min'] = g_low.min(axis=1)
    gamma = (g_high['max'] - g_low['min'])**2
    ln_gamma = np.log(g_high['max']/g_low['min'])**2
    
    alpha, s = calculate_alpha_s(beta, gamma)
    ln_alpha, ln_s = calculate_alpha_s(ln_beta, ln_gamma)

    table['beta'] = beta
    table['gamma'] = gamma
    table['alpha'] = alpha
    table['s'] = s
    table['ln_alpha'] = ln_alpha
    table['ln_s'] = ln_s
    table['mask'] = s > 0
    table['ln_mask'] = ln_s > 0

    output = pd.concat({'quarter': table['quarter'], 's': table['s']*table['mask'], 'ln_s': table['ln_s']*table['ln_mask']}, axis=1)
    output = output.groupby('quarter').mean()
    output.to_csv(output_file)

def main():
    codes = None
    with open('codes.txt', 'r') as fd:
        codes = fd.readlines()
    codes = [c[:3] for c in codes]

    for code in tqdm(codes):
        process(code)

if __name__ == '__main__':
    main()
    # process('HPG')
