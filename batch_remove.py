import os
from tqdm import tqdm

pattern = './rawdata/{}/events.csv'

def main():
    codes = None
    with open('codes.txt', 'r') as fd:
        codes = fd.readlines()
    codes = [c[:3] for c in codes]

    for c in tqdm(codes):
        path = pattern.format(c)
        if os.path.isfile(path):
            os.remove(path)
        else:
            print("file {} not found".format(path))

if __name__ == '__main__':
    main()
