try: 
    from BeautifulSoup import BeautifulSoup
except ImportError:
    from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
import requests
import json

class VolumeLoader():
    def __init__(self, htmlfile):
        cafef_fmt = 'https://s.cafef.vn{}'
        html = None
        with open(htmlfile, 'r') as fd:
            html = fd.read()
        html = BeautifulSoup(html, features="lxml")
        all_tr = html.find_all('tr')
        all_tr = all_tr[1:]
        self.__stock_url = dict()
        for tr in all_tr:
            a = tr.find_all('a', href=True)
            a0 = a[0]
            code = a0.contents[0]
            url = cafef_fmt.format(a0['href'])
            self.__stock_url[code] = url

        self.__vietstock_url = 'https://finance.vietstock.vn/data/eventstypedata'
        self.__vietstock_header = {
            'Cookie': '_ga=GA1.2.1461040072.1664899465; Theme=Light; AnonymousNotification=; _gid=GA1.2.1218471786.1666446518; language=vi-VN; ASP.NET_SessionId=lrecevyl5mll3m3h5i5a2joe; finance_viewedstock=AAA,; __RequestVerificationToken=f5pHNETdKpCuaWZQMCPyNt6uaKRSMH31wdkWU0ew2_-J0W3aMHBL_6N_m58yBxATUT6rWGmv4qT8bTsAZHFbzs7wKwGj85u1G1bpeTEuc6s1',
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        self.__vietstock_query_fmt = 'eventTypeID=2&channelID=17&code={}&catID=&fDate=&tDate=&page=1&pageSize=500&orderBy=Date1&orderDir=DESC&__RequestVerificationToken=8dtHS-mvJ4qVqy1efXaJ_g_oJgOn6YAFsI4QOw0uAndchYCaS4bc5don-Lv6MXtt3oeWZzx0EEGXlRd0CPJy4DTWFfEsJHUYqQsJhx-IOYk1'
        return

    def load_from_cafef(self, code):
        if code not in self.__stock_url:
            raise Exception("stock code {} not exist on cafef".format(code))

        response = requests.get(self.__stock_url[code])
        html = BeautifulSoup(response.text, features="lxml")
        div = html.find_all('div', {'class': 'dltl-other'})
        div_start = div[0]
        div_recent = div[2]

        div_start = div_start.find_all('div')
        start_date = None
        start_stock_volume = None
        recent_stock_volume = None

        for sd in div_start:
            contents = sd.contents
            if len(contents) < 2:
                continue
            if 'Ngày giao dịch đầu tiên:' in contents[0]:
                start_date_str = contents[1].contents[0]
                chunks = start_date_str.strip().split('/')
                if len(chunks) == 3:
                    start_date = '-'.join([chunks[2], chunks[1], chunks[0]])
            elif 'Khối lượng cổ phiếu niêm yết lần đầu:' in contents[0]:
                start_stock_volume_str = contents[1].contents[0]
                start_stock_volume_str = start_stock_volume_str.strip().replace(',', '')
                start_stock_volume = int(start_stock_volume_str)

        div_recent = div_recent.find_all('div')
        contents = list()
        for sd in div_recent:
            contents.extend(sd.contents)

        for i, c in enumerate(contents):
            if 'KLCP đang lưu hành:' in c:
                num_str = contents[i+1].strip().replace(',', '')
                recent_stock_volume = int(num_str)

        return {
            'start_date': start_date,
            'start_stock_volume': start_stock_volume,
            'recent_stock_volume': recent_stock_volume,
        }

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
#   -H 'Referer: https://finance.vietstock.vn/lich-su-kien.htm?page=1&tab=2&code=AAA&group=17' \
#   -H 'Accept-Language: en-US,en;q=0.9' \
#   -H 'Cookie: _ga=GA1.2.1461040072.1664899465; Theme=Light; AnonymousNotification=; _gid=GA1.2.1218471786.1666446518; language=vi-VN; ASP.NET_SessionId=lrecevyl5mll3m3h5i5a2joe; finance_viewedstock=AAA,; __RequestVerificationToken=f5pHNETdKpCuaWZQMCPyNt6uaKRSMH31wdkWU0ew2_-J0W3aMHBL_6N_m58yBxATUT6rWGmv4qT8bTsAZHFbzs7wKwGj85u1G1bpeTEuc6s1' \
#   --data-raw 'eventTypeID=2&channelID=17&code=AAA&catID=&fDate=&tDate=&page=1&pageSize=20&orderBy=Date1&orderDir=DESC&__RequestVerificationToken=8dtHS-mvJ4qVqy1efXaJ_g_oJgOn6YAFsI4QOw0uAndchYCaS4bc5don-Lv6MXtt3oeWZzx0EEGXlRd0CPJy4DTWFfEsJHUYqQsJhx-IOYk1' \
#   --compressed

    def load_from_vietstock(self, code):
        response = requests.post(self.__vietstock_url, headers=self.__vietstock_header, data=self.__vietstock_query_fmt.format(code))
        json_text = response.text
        text_without_bom = json_text.encode().decode('utf-8-sig')
        if len(text_without_bom) == 0:
            return None
        data = json.loads(text_without_bom)
        # print(text_without_bom)
        rows, count = data[0], data[1][0]
        all_items = list()
        for row in rows:
            date_ctgd = row['DateCTGD']
            if date_ctgd is not None:
                # lấy ngày niêm yết thôi
                continue
            volume = row["Volume"]

            timestamp = int(row['DateOrder'][6:-5])
            date = datetime.fromtimestamp(timestamp, tz=timezone(timedelta(hours=7)))
            item = {
                'timestamp': timestamp,
                'volume': volume,
                'date': '{}-{:02d}-{:02d}'.format(date.year, date.month, date.day)
            }
            all_items.append(item)

        if len(all_items) == 0:
            return None

        all_items = sorted(all_items, key=lambda x: x['timestamp'])
        soonest = all_items[0]
        return {
            'start_date': soonest['date'],
            'start_stock_volume': soonest['volume'],
            'recent_stock_volume': None,
        }

if __name__ == '__main__':
    loader = VolumeLoader('../rawdata/cafef_stock_page.html')
    data = loader.load_from_cafef('AAA')
    print(data)
    data = loader.load_from_vietstock('AAA')
    print(data)
