import os
import pandas as pd
from tqdm import tqdm

input_finance_fmt = './rawdata/{}/finance.csv'
output_quarterly_shares_fmt = './processed/shares_est/{}/shares_est.csv'
output_folder_fmt = './processed/shares_est/{}'

def main():
    codes = None
    with open('codes.txt', 'r') as fd:
        codes = fd.readlines()
    codes = [c[:3] for c in codes]

    for code in tqdm(codes):
        print(code)
        input_file = input_finance_fmt.format(code)
        folder = output_folder_fmt.format(code)
        output_file = output_quarterly_shares_fmt.format(code)
        os.makedirs(folder, exist_ok=True)
        if os.path.isfile(output_file):
            continue

        fin = pd.read_csv(input_file)
        fin = fin.drop(fin.columns[0], axis=1)
        columns = fin.columns
        iassests = None
        idebt = None
        ibvps = None
        iequity = None
        for i in range(len(columns)):
            c = columns[i]
            if 'Tổng tài sản' in c:
                iassests = i
            elif 'Nợ phải trả' in c:
                idebt = i
            elif 'BVPS' in c:
                ibvps = i
            elif 'Vốn chủ sở hữu' in c:
                iequity = i

        owner_equity = None
        if ibvps is None:
            raise Exception("bvps not found")
        if iequity is not None:
            owner_equity = fin[columns[iequity]]
        elif iassests is not None and idebt is not None:
            assests = fin[columns[iassests]]
            debt = fin[columns[idebt]]
            owner_equity = assests - debt
        else:
            raise Exception("owner_equity not found")

        quarter = fin['Quý']
        bvps = fin[columns[ibvps]]
        shares_est = 1000000*owner_equity/bvps
        shares_est_diff = shares_est.diff()
        output = pd.DataFrame({
            'quarter': quarter, 
            'owner_equity': owner_equity, 
            'bvps': bvps, 
            'shares_est': shares_est,
            'shares_est_diff': shares_est_diff,
            'shares_est_diff_perc': 100*shares_est_diff/shares_est
        })
        output.to_csv(output_file)

if __name__ == '__main__':
    main()
