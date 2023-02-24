import requests
from bs4 import BeautifulSoup
import pymysql
import time
import datetime

start_time = time.time()
now_time = datetime.datetime.now()

now_time_utc = datetime.datetime.utcnow()
now_time_str = now_time_utc.strftime('%H%M')

h_now = int(now_time_str[:2])
m_now = int(now_time_str[2:])

if h_now == 18 & m_now < 20:
    print("초기 db구축 시간임")
else:
    print("작업시행")

print(now_time, "작업 시작")
print(h_now, m_now)





'''
host = ''
username = ''
password = ''
database = ''

aws_db = pymysql.connect(
    host=host,
    port=3306,
    user=username,
    passwd=password,
    db=database,
    charset='utf8',
)

cursor = aws_db.cursor()


url = ""
key = ""

#작업1 : 오전 3시 18분에 20분전 업데이트 충전소 변경된 데이터 업데이트

#분단위
period = 20

params = "&period=%d" % (period)
open_url = url + key + params

res = requests.get(open_url)
soup = BeautifulSoup(res.content, 'html.parser')
resultcount = soup.find_all('resultcount')
data = soup.find_all('item')

print(resultcount)

temp = []

for item in data:
    stat = item.find('stat').text
    statupddt = item.find('statupddt').text
    statid = item.find('statid').text
    chgerid = item.find('chgerid').text
    temp.append([stat, statupddt, statid, chgerid])

sql = "UPDATE elecgrdb SET stat=%s, statupddt=%s WHERE statid=%s AND chgerid=%s"

cursor.executemany(sql, temp)
aws_db.commit()

print(len(temp), temp)

print("--- 총 %s 초 소요됨. 1번째 작업완료! ---" % (time.time() - start_time))



#작업2 : 오전 3시 18분에 20분전 업데이트 충전소 변경된 데이터 업데이트

#분단위
period = 3

params = "&period=%d" % (period)
open_url = url + key + params

res = requests.get(open_url)
soup = BeautifulSoup(res.content, 'html.parser')
resultcount = soup.find_all('resultcount')
data = soup.find_all('item')

print(resultcount)

temp = []

for item in data:
    stat = item.find('stat').text
    statupddt = item.find('statupddt').text
    statid = item.find('statid').text
    chgerid = item.find('chgerid').text
    temp.append([stat, statupddt, statid, chgerid])

sql = "UPDATE elecgrdb SET stat=%s, statupddt=%s WHERE statid=%s AND chgerid=%s"

cursor.executemany(sql, temp)
aws_db.commit()


cursor.close()
aws_db.close()
print("--- 총 %s 초 소요됨. 2번째 작업완료! ---" % (time.time() - start_time))

'''

