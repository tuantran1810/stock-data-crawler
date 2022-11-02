import os
import pandas as pd
from tqdm import tqdm

input_price_fmt = './rawdata/{}/price_daily.csv'
output_ret_quarterly_fmt = './processed/{}/ret_quarterly.csv'
output_folder_fmt = './processed/{}'

valid_date = [('31', '3'), ('30', '6'), ('30', '9'), ('31', '12')]

def get_day_month_year(s: str):
    year, month, day = s.split('-')
    return (day, month, year)

def month_year_to_quarter(month: str, year:str):
    m = int(month)
    q = int((m+2)/3)
    return '{}Q{}'.format(year, q)

def process(code: str):
    print(code)
    input_price = input_price_fmt.format(code)
    folder = output_folder_fmt.format(code)
    output_file = output_ret_quarterly_fmt.format(code)

    os.makedirs(folder, exist_ok=True)
    if os.path.isfile(output_file):
        return

    price = pd.read_csv(input_price)
    price = price.drop([0, 1], axis=0)
    date = price.loc[:, 'Attributes']
    date_price = price.loc[:, 'close']

    length = len(date)
    all_items = dict()
    ldate = date.iloc[0]
    lprice = date_price.iloc[0]
    lday, lmonth, lyear = get_day_month_year(ldate)
    for i in range(1, length):
        idate = date.iloc[i]
        iprice = date_price.iloc[i]
        day, month, year = get_day_month_year(idate)

        if (day, month) in valid_date:
            q_str = month_year_to_quarter(month, year)
            item = {
                'date': idate,
                'quarter': q_str,
                'price': iprice,
            }
            all_items[q_str] = item
        elif month in ('04', '07', '10', '01') and month != lmonth:
            q_str = month_year_to_quarter(lmonth, lyear)
            item = {
                'date': ldate,
                'quarter': q_str,
                'price': lprice,
            }
            all_items[q_str] = item
        elif month_year_to_quarter(lmonth, lyear) != month_year_to_quarter(month, year):
            q_str = month_year_to_quarter(lmonth, lyear)
            item = {
                'date': ldate,
                'quarter': q_str,
                'price': lprice,
            }
            all_items[q_str] = item
        lday, lmonth, lyear, lprice, ldate = day, month, year, iprice, idate

    arr = list()
    for _, v in all_items.items():
        arr.append(v)
    arr = sorted(arr, key=lambda x: x['date'])
    quarter_price = pd.DataFrame(arr)
    quarter_price['price'] =  quarter_price['price'].astype(float)
    tmp = {'date': '', 'quarter': '', 'price': float(date_price.iloc[0])}
    quarter_price = pd.DataFrame([tmp]).append(quarter_price)
    # print(quarter_price['price'].diff())
    quarter_price['price_diff'] = quarter_price['price'].diff()
    quarter_price['pre_price'] = quarter_price['price'].shift(1)
    quarter_price['price_diff_perc'] = 100.0*quarter_price['price_diff']/quarter_price['pre_price']
    quarter_price = quarter_price.drop([0], axis=0)
    quarter_price = quarter_price.drop(['date', 'pre_price'], axis=1)
    quarter_price.to_csv(output_file)

def main():
    codes = None
    with open('codes.txt', 'r') as fd:
        codes = fd.readlines()
    codes = [c[:3] for c in codes]

    for code in tqdm(codes):
        process(code)

if __name__ == '__main__':
    main()
    # process('AAA')
