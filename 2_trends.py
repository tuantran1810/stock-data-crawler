import os
import time
from tqdm import tqdm
from pytrends.request import TrendReq

output_daily_fmt = './rawdata/{}/trend.csv'
output_folder_fmt = './rawdata/{}'

def main():
    codes = None
    with open('codes.txt', 'r') as fd:
        codes = fd.readlines()
    codes = [c[:3] for c in codes]

    pytrends = TrendReq(hl='en-US', tz=360)

    for c in tqdm(codes):
        daily_file = output_daily_fmt.format(c)
        folder = output_folder_fmt.format(c)
        os.makedirs(folder, exist_ok=True)
        if os.path.isfile(daily_file):
            continue
        kw_list = [c] # list of keywords to get data 
        pytrends.build_payload(kw_list, cat=0, timeframe='2005-01-01 2022-09-30', geo='VN') 
        data = pytrends.interest_over_time()
        data = data.reset_index()
        data.to_csv(daily_file)
        time.sleep(12)

if __name__ == '__main__':
    main()
