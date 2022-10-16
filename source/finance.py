from curses import meta
import requests
import pandas as pd
import logging as logging
import requests
from copy import deepcopy

# metadata = {
#     "ID": 4,
#     "Row": 4,
#     "CompanyID": 686,
#     "YearPeriod": 2021,
#     "TermCode": "Q3",
#     "TermName": "Quý 3",
#     "TermNameEN": "Quarter 3",
#     "ReportTermID": 4,
#     "DisplayOrdering": 18,
#     "United": "HN",
#     "AuditedStatus": "SX",
#     "PeriodBegin": "202107",
#     "PeriodEnd": "202109",
#     "TotalRow": 70,
#     "BusinessType": 1,
#     "ReportNote": null,
#     "ReportNoteEn": null
# }

class FinanceLoader():
    def __init__(self):
        self.__url = 'https://finance.vietstock.vn/data/financeinfo'
        self.__header = {
            'Cookie': 'language=vi-VN; ASP.NET_SessionId=0hhnfhm2q4iccolx3xuzri33; __RequestVerificationToken=fGfQJNh25ZaRjLKuwwN_RZOJct1Dp-pUvVJIDkpPC-zn0TDB48D2_cCssAtBD9Lp3UG5JR0tzxsO03YVoYve6U-efjPsB4PovrHvOji8-s01; Theme=Light; _ga=GA1.2.1506579389.1665851329; _gid=GA1.2.531154118.1665851329; AnonymousNotification=; __gpi=UID=00000b630d6f0b98:T=1665851330:RT=1665851330:S=ALNI_MZaX_Gf9OF4B-0snAcjla83gr1kiQ; dable_uid=84027835.1665851331171; _cc_id=2d34c147c3b3fd9e7fbf72fd5f243aec; panoramaId_expiry=1666456132614; panoramaId=c12781f6cbe27236ce01f5f873814945a702655d05e1edd25cdc8f99c343b90f; __gads=ID=b32e9c1719684c4c-22a6d21e10d70042:T=1665851330:S=ALNI_MYoF0uHq3q1JH76BueYDqIUJYtKUg; cto_bundle=2rkiaV9vbVZ4Q0xZemYzM1hWOTVzYnpYZFVabmxlTG5tOGd6MnRPaTNQOUozR1VPQ1VxMCUyRllvNHFQQWVydUZEQUhFRXJLMmFJWG45UWcxcSUyRkowM0xrODR4MXJiNkZLbHgzcHJJJTJGWkhsWlk5JTJCSHkxV3ZPRFFBTWRIeEcyYVhGRTZ3Nm1zTmRjdHFmT0NvM1UyeGRUdDNkekxKWThjSGc2OFROT3F5cEFzaUQ2JTJGZjJNNnBuWTdIV3RmbnhUaE1SSVRMSnhTQXhMdGJiUXpZVXUzZmVXMkhaWEFPUSUzRCUzRA; finance_viewedstock=VND,HPG,',
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        }
        self.__query_fmt = 'Code={}&Page={}&PageSize=2&ReportTermType=2&ReportType=BCTQ&Unit=1000000&__RequestVerificationToken=E_G8hcQ_01qrbuSONKA37Lv2IJYVC0lY05cv0ADPlGKJjrLVhMOJa49SxXHYCzRg3zHTMmFg3SJxUxt5IUcrbblIWSb4BNaZFB5--WbUne41'
        return

    def get_vietstock_data(self, code, n_years=1):
        all_stock_quarterly_data = dict()
        for page in range(1, n_years+1):
            response = requests.post(self.__url, headers=self.__header, data=self.__query_fmt.format(code, page))
            data = response.json()
            metadata = data[0]
            details = data[1]
            if len(metadata) == 0:
                break

            stock_quarterly_data_arr = [None] * (len(metadata) + 1)
            for m in metadata:
                index = m['ID']
                term_code = m['TermCode']
                stock_quarterly_data = dict()
                stock_quarterly_data['report_term_id'] = m['ReportTermID']
                stock_quarterly_data['time_order'] = m['YearPeriod']*10 + int(term_code[1])
                stock_quarterly_data['period_string'] = '{}{}'.format(m['YearPeriod'], term_code)
                stock_quarterly_data_arr[index] = stock_quarterly_data

            for term in ['Kết quả kinh doanh', 'Cân đối kế toán', 'Chỉ số tài chính']:
                stats_items = details[term]
                for item in stats_items:
                    name = item['Name']
                    unit = 'MilVND' if len(item['UnitEn']) == 0 else item['UnitEn']
                    name_with_unit = '{}({})'.format(name, unit)
                    for i in range(1, len(stock_quarterly_data_arr)):
                        txt = 'Value{}'.format(i)
                        value = item[txt]
                        stock_quarterly_data_arr[i][name_with_unit] = value

            for stock_quarterly_data in stock_quarterly_data_arr[1:]:
                time_order = stock_quarterly_data['time_order']
                all_stock_quarterly_data[time_order] = stock_quarterly_data
        
        order_nums = all_stock_quarterly_data.keys()
        sorted_order_nums = sorted(order_nums)
        ordered_data = list()
        for key in sorted_order_nums:
            ordered_data.append(all_stock_quarterly_data[key])

        data = pd.DataFrame(ordered_data)
        data = data.drop(['report_term_id', 'time_order'], axis=1)
        data = data.rename({'period_string': 'Quý'}, axis=1)
        return data
