import os
import time
from tqdm import tqdm
from source.events import EventsLoader

output_fmt = './rawdata/{}/events.csv'
output_folder_fmt = './rawdata/{}'

def main():
    codes = None
    with open('codes.txt', 'r') as fd:
        codes = fd.readlines()
    codes = [c[:3] for c in codes]

    for c in tqdm(codes):
        output_file = output_fmt.format(c)
        folder = output_folder_fmt.format(c)
        os.makedirs(folder, exist_ok=True)
        if os.path.isfile(output_file):
            continue
        print(c)
        loader = EventsLoader()
        data = loader.get_vietstock_data(c)
        data.to_csv(output_file)
        time.sleep(0.1)

if __name__ == '__main__':
    main()
