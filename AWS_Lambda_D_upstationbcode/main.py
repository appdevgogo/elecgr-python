from concurrent.futures import ThreadPoolExecutor
import requests
from bs4 import BeautifulSoup
import pymysql
import concurrent.futures
import time
import datetime

#공공데이터에서 받은 충전소 위경도로 네이버 API활용하여 법정동 코드 추출후 AWS에 저장
#AWS_Lambda_D_upstationbcode (매일, 공공데이터 업로드 후에 실행)
#30분에 한번씩 2번에 걸쳐서 시행필요()

start_time = time.time()
now_time = datetime.datetime.now()
print(now_time, " 에 법정동 코드 입력 작업시작. ", "진행중.. 약 16분정도 소요예정")

host = ''
username = ''
password = ''
database = ''

aws_db = pymysql.connect(
    host=host,  # DATABASE_HOST
    port=3306,
    user=username,  # DATABASE_USERNAME
    passwd=password,  # DATABASE_PASSWORD
    db=database,  # DATABASE_NAME
    charset='utf8'
)

key = ""
ps = ""

cursor = aws_db.cursor()

sql = "SELECT statid, lat, lng FROM elecgrtempdb WHERE addcd IS NULL" #LIMIT 0, 100 / WHERE addcd IS NULL
cursor.execute(sql)
result = cursor.fetchall()

geoURLS = {}
temp = []

for item in result:
    statid = item[0]
    geocode = str(item[2])+","+str(item[1])
    furl = "" % (key, ps, geocode)
    geoURLS[furl] = statid


def load_url(url):
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'html.parser')
    code = soup.find('id').text
    return code

with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    future_to_url = {executor.submit(load_url, url): url for url in geoURLS.keys()}
    for future in concurrent.futures.as_completed(future_to_url):
        url = future_to_url[future]
        try:
            data = future.result()
            #print(data, geoURLS.get(url))
            temp.append([data, geoURLS.get(url)])

        except Exception as e:
            blank = "0"
            #print(blank, geoURLS.get(url))
            temp.append([blank, geoURLS.get(url)])

print("%s 초 소요됨. 법정동코드 네이버 RGEO에서 추출완료. " % (time.time() - start_time), "다음작업 진행중..")

num = len(temp)
idx_first = 0
idx_last = 0
pagesize = 1000
interval = pagesize - 1

count = int(num / pagesize) + 1

for i in range(0, count):
    print(i)
    idx_last = idx_first + interval
    print(idx_first, idx_last)
    array_final = temp[idx_first:idx_last]
    idx_first += pagesize
    sql_final = "UPDATE elecgrtempdb SET addcd = %s WHERE statid = %s"
    cursor.executemany(sql_final, array_final)
    aws_db.commit()

cursor.close()
aws_db.close()

print("%s 초 소요됨. 작업완료. " % (time.time() - start_time))
