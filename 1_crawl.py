import os
from tqdm import tqdm
import source.dataloader as dt
import source.finance as fn

output_daily_fmt = './rawdata/{}/price_daily.csv'
output_quarterly_finance_fmt = './rawdata/{}/finance.csv'
output_folder_fmt = './rawdata/{}'

def main():
    codes = None
    with open('codes.txt', 'r') as fd:
        codes = fd.readlines()

    codes = [c[:3] for c in codes]
    for c in tqdm(codes):
        daily_file = output_daily_fmt.format(c)
        quarterly_finance_file = output_quarterly_finance_fmt.format(c)
        folder = output_folder_fmt.format(c)
        os.makedirs(folder, exist_ok=True)
        if os.path.isfile(daily_file) and os.path.isfile(quarterly_finance_file):
            continue

        price_dataloader = dt.DataLoader(
            symbols=c,
            start='2005-01-01',
            end='2022-09-30',
            minimal=False,
            data_source='vnd',
        )
        finance_dataloader = fn.FinanceLoader()

        try:
            price_data = price_dataloader.download()
            finance_data = finance_dataloader.get_vietstock_data(c, 19)
            price_data.to_csv(daily_file)
            finance_data.to_csv(quarterly_finance_file)
        except Exception as e:
            print('error with code {}, exception = {}'.format(c, e))

if __name__ == '__main__':
    main()
