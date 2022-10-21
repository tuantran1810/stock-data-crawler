import os
import pandas as pd

output_daily_trend_fmt = './processed/aggregated/{}/trend.csv'
output_daily_price_fmt = './processed/aggregated/{}/price_daily.csv'
output_quarterly_finance_fmt = './processed/aggregated/{}/finance.csv'
output_folder_fmt = './processed/aggregated/{}'

input_daily_trend_fmt = './rawdata/{}/trend.csv'
input_daily_price_fmt = './rawdata/{}/price_daily.csv'
input_quarterly_finance_fmt = './rawdata/{}/finance.csv'
input_folder_fmt = './rawdata/{}'

def check_file_exist(file: str) -> bool:
    return os.path.isfile(file)

def process_finance(input_path: str, output_path: str) -> None:
    if check_file_exist(output_path):
        return
    if not check_file_exist(input_path):
        return
    return

def process_daily_price(input_path: str, output_path: str) -> None:
    if check_file_exist(output_path):
        return
    if not check_file_exist(input_path):
        return
    return

def process_daily_trend(input_path: str, output_path: str) -> None:
    if check_file_exist(output_path):
        return
    if not check_file_exist(input_path):
        return
    df = pd.read_csv(input_path)
    df = df.drop(df.columns[[0,3]], axis=1)
    # print(df.head())
    all_data = dict()
    for _, row in df.iterrows():
        date_str, cnt = row[0], row[1]
        tmp = date_str.split('-')
        year = int(tmp[0])
        month = int(tmp[1])
        quarter = 1 + int((month-0.99)/3)
        index = year*10 + quarter
        if index not in all_data:
            all_data[index] = {
                'year': year,
                'quarter': quarter,
                'value': cnt
            }
        else:
            all_data[index]['value'] = all_data[index]['value'] + cnt

    max_value = 0
    for k in all_data.keys():
        item = all_data[k]
        value = item['value']
        max_value = max(max_value, value)

    arr = list()
    for k in sorted(all_data.keys()):
        item = all_data[k]
        item['value'] = int(item['value']*100/max_value)
        arr.append(item)
    
    df = pd.DataFrame(arr)
    df.to_csv(output_path)
    return

def process_one(code: str) -> None:
    input_folder = input_folder_fmt.format(code)
    input_daily_trend = input_daily_trend_fmt.format(code)
    input_daily_price = input_daily_price_fmt.format(code)
    input_quarterly_finance = input_quarterly_finance_fmt.format(code)

    output_folder = output_folder_fmt.format(code)
    output_daily_trend = output_daily_trend_fmt.format(code)
    output_daily_price = output_daily_price_fmt.format(code)
    output_quarterly_finance = output_quarterly_finance_fmt.format(code)

    os.makedirs(output_folder, exist_ok=True)

    process_finance(input_quarterly_finance, output_daily_trend)
    process_daily_price(input_daily_price, output_daily_price)
    process_daily_trend(input_daily_trend, output_daily_trend)

def main():
    process_one('HPG')

if __name__ == '__main__':
    main()
