from datetime import datetime, timezone, timedelta
import pandas as pd
import requests
import json

# curl 'https://finance.vietstock.vn/data/eventstypedata' \
#   -H 'Connection: keep-alive' \
#   -H 'sec-ch-ua: "Chromium";v="94", "Microsoft Edge";v="94", ";Not A Brand";v="99"' \
#   -H 'Accept: */*' \
#   -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
#   -H 'X-Requested-With: XMLHttpRequest' \
#   -H 'sec-ch-ua-mobile: ?0' \
#   -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.50 Safari/537.36 Edg/94.0.992.23' \
#   -H 'sec-ch-ua-platform: "Linux"' \
#   -H 'Origin: https://finance.vietstock.vn' \
#   -H 'Sec-Fetch-Site: same-origin' \
#   -H 'Sec-Fetch-Mode: cors' \
#   -H 'Sec-Fetch-Dest: empty' \
#   -H 'Referer: https://finance.vietstock.vn/lich-su-kien.htm?page=2&tab=2&code=AAA' \
#   -H 'Accept-Language: en-US,en;q=0.9' \
#   -H 'Cookie: _ga=GA1.2.1461040072.1664899465; Theme=Light; AnonymousNotification=; _gid=GA1.2.1218471786.1666446518; language=vi-VN; ASP.NET_SessionId=lrecevyl5mll3m3h5i5a2joe; __RequestVerificationToken=f5pHNETdKpCuaWZQMCPyNt6uaKRSMH31wdkWU0ew2_-J0W3aMHBL_6N_m58yBxATUT6rWGmv4qT8bTsAZHFbzs7wKwGj85u1G1bpeTEuc6s1; finance_viewedstock=AAA,AAM,ABT,' \
#   --data-raw 'eventTypeID=2&channelID=0&code=AAA&catID=&fDate=&tDate=&page=1&pageSize=20&orderBy=Date1&orderDir=DESC&__RequestVerificationToken=TpiOFuRHKSlQGxLBu4pyF6uYWDEKMYufqDm8wIIOBKx5szMB0WdYMlT4rfa8MjTko1chmXCn1qvNU6yZuj2ikDlRFHEe19-UOkfJ83wn9G41' \
#   --compressed

class EventsLoader():
    def __init__(self):
        self.__url = 'https://finance.vietstock.vn/data/eventstypedata'
        self.__header = {
            'Cookie': '_ga=GA1.2.1461040072.1664899465; Theme=Light; AnonymousNotification=; _gid=GA1.2.1218471786.1666446518; language=vi-VN; ASP.NET_SessionId=lrecevyl5mll3m3h5i5a2joe; __RequestVerificationToken=f5pHNETdKpCuaWZQMCPyNt6uaKRSMH31wdkWU0ew2_-J0W3aMHBL_6N_m58yBxATUT6rWGmv4qT8bTsAZHFbzs7wKwGj85u1G1bpeTEuc6s1; finance_viewedstock=AAA,AAM,ABT,',
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        self.__query_fmt = 'eventTypeID=2&channelID=0&code={}&catID=&fDate=&tDate=&page={}&pageSize=50&orderBy=Date1&orderDir=DESC&__RequestVerificationToken=TpiOFuRHKSlQGxLBu4pyF6uYWDEKMYufqDm8wIIOBKx5szMB0WdYMlT4rfa8MjTko1chmXCn1qvNU6yZuj2ikDlRFHEe19-UOkfJ83wn9G41'

    def __get_vietstock_data(self, code, page):
        response = requests.post(self.__url, headers=self.__header, data=self.__query_fmt.format(code, page))
        json_text = response.text
        text_without_bom = json_text.encode().decode('utf-8-sig')
        if len(text_without_bom) == 0:
            return [], False
        data = json.loads(text_without_bom)
        # print(text_without_bom)
        rows, count = data[0], data[1][0]
        all_items = list()
        for row in rows:
            # date_ctgd = row['DateCTGD']
            # if date_ctgd is not None:
            #     # lấy ngày niêm yết thôi
            #     continue
            volume = row["Volume"]
            note = row["Note"]

            timestamp = int(row['DateOrder'][6:-5])
            date = datetime.fromtimestamp(timestamp, tz=timezone(timedelta(hours=7)))
            item = {
                'timestamp': timestamp,
                'delta_volume': volume,
                'date': '{}-{:02d}-{:02d}'.format(date.year, date.month, date.day),
                'note': note,
            }
            all_items.append(item)
        last_row = rows[-1]
        return all_items, last_row['Row'] <= count

    def get_vietstock_data(self, code):
        all_items = list()
        page = 0
        while True:
            page += 1
            items, keep_going = self.__get_vietstock_data(code, page)
            all_items.extend(items)
            if not keep_going:
                break
            
        if len(all_items) == 0:
            return pd.DataFrame(columns=['','timestamp','delta_volume','date'])
        all_items = sorted(all_items, key=lambda x: x['timestamp'])
        data = pd.DataFrame(all_items)
        return data

if __name__ == '__main__':
    loader = EventsLoader()
    data = loader.get_vietstock_data('AAA')
    print(data)
