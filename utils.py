import os
import csv
from tqdm import tqdm

raw_events_fmt = './rawdata/{}/events.csv'
shares_est_fmt = './processed/{}/shares_est.csv'
stdv_fmt = './processed/{}/stdv.csv'
amihud_fmt = './processed/{}/amihud.csv'
quarterly_marker_cap_fmt = './rawdata/{}/price_daily.csv'

def remove_file(path_fmt, codes):
    for c in tqdm(codes):
        path = path_fmt.format(c)
        if os.path.isfile(path):
            os.remove(path)
        else:
            print("file {} not found".format(path))

def from_q3_2007(path_fmt, codes):
    arr = list()
    narr = list()
    for c in tqdm(codes):
        path = path_fmt.format(c)
        if os.path.isfile(path):
            with open(path) as fd:
                f = csv.reader(fd, delimiter=',')
                in_range = False
                for i, row in enumerate(f):
                    if i < 3: continue
                    date = row[0]
                    ys, ms, ds = date.split('-')
                    y, m, d = int(ys), int(ms), int(ds)
                    n = y*10000 + m*100 +d
                    if n >= 20070630 and n <= 20140000:
                        in_range = True
                        break
                if in_range:
                    arr.append(c)
                else:
                    narr.append(c)
        else:
            print("file {} not found".format(path))
    print(sorted(arr))
    print(sorted(narr))
    print(len(arr))
    print(len(narr))

def main():
    codes = None
    with open('codes.txt', 'r') as fd:
        codes = fd.readlines()
    codes = [c[:3] for c in codes]

    # remove_file(raw_events_fmt, codes)
    # remove_file(shares_est_fmt, codes)
    # remove_file(stdv_fmt, codes)
    remove_file(amihud_fmt, codes)
    # from_q3_2007(quarterly_marker_cap_fmt, codes)


if __name__ == '__main__':
    main()
