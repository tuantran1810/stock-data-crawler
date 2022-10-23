from source.volume import VolumeLoader
import pandas as pd

input_fmt = './rawdata/{}/events.csv'
output_fmt = './rawdata/{}/volume.csv'
output_folder_fmt = './rawdata/{}'

def main():
    codes = None
    with open('codes.txt', 'r') as fd:
        codes = fd.readlines()
    codes = [c[:3] for c in codes]

    loader = VolumeLoader('./rawdata/cafef_stock_page.html')
    for code in codes[:20]:
        # print(code)
        data_cafef = loader.load_from_cafef(code)
        start_date = data_cafef['start_date']
        start_stock_volume = data_cafef['start_stock_volume']
        recent_stock_volume = data_cafef['recent_stock_volume']
        if data_vietstock is not None:
            tmp_start_date = data_vietstock['start_date']
            tmp_start_stock_volume = data_vietstock['start_stock_volume']
            if start_date is None or tmp_start_date < start_date:
                start_date = tmp_start_date
                start_stock_volume = data_vietstock['start_stock_volume']

        # print(start_date, start_stock_volume, recent_stock_volume)
        input_events = pd.read_csv(input_fmt.format(code))
        input_events = input_events.drop(input_events.columns[0], axis=1)
        s = input_events['delta_volume'].sum() + start_stock_volume
        print(code, s == recent_stock_volume, start_date, start_stock_volume, recent_stock_volume, s)

if __name__ == '__main__':
    main()
