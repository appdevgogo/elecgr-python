import requests
from bs4 import BeautifulSoup
import pymysql
import time
import datetime

#네이버 법정동 코드 추출작업이 원활하게 되지 않는 충전소의 주소를 받아와 위경도를 수정하고 위경도 기준 다시 추출

start_time = time.time()
now_time = datetime.datetime.now()
print(now_time, " 에 법정동 코드 수정 작업시작. ", "진행중.. 약 ?분정도 소요예정")


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


#----> 법정동 코드 0값 위경도 수정 작업
sql = "SELECT statid, addr FROM elecgrtempdb WHERE addcd = '0' OR addcd IS NULL"  # LIMIT 0, 100 / WHERE addcd IS NULL
cursor.execute(sql)
result = cursor.fetchall()

temp = []

for item in result:
    statid = item[0]
    add = item[1]

    furl = "" % (
    key, ps, add)

    res = requests.get(furl).json()
    lat = res['addresses'][0]['y']
    lng = res['addresses'][0]['x']

    temp.append([lat, lng, statid])

print(temp)

sql_final = "UPDATE elecgrtempdb SET lat = %s, lng = %s WHERE statid = %s"
cursor.executemany(sql_final, temp)
aws_db.commit()

print("%s초 소요됨. 법정동코드 0 및 NULL값 위경도 수정완료. " % (time.time() - start_time), "다음작업 진행중..")



#----> 법정동코드 0 및 NULL값 수정된 충전소 위경도 기준으로 법정동 재추출 작업
sql = "SELECT statId, lat, lng FROM elecgrtempdb WHERE addcd = '0' OR addcd IS NULL"  # LIMIT 0, 100 / WHERE addCd IS NULL
cursor.execute(sql)
result = cursor.fetchall()

temp = []

for item in result:
    statid = item[0]
    geocode = str(item[2]) + "," + str(item[1])
    furl = "" % (
    key, ps, geocode)
    res = requests.get(furl)
    soup = BeautifulSoup(res.content, 'html.parser')
    code = soup.find('id').text
    x = soup.find('x').text
    print(code, x)
    temp.append([code, statid])

sql_final = "UPDATE elecgrtempdb SET addcd = %s WHERE statid = %s"
cursor.executemany(sql_final, temp)
aws_db.commit()

print("%s초 소요됨. 법정동코드 NULL값 재처리 및 수정 작업완료. " % (time.time() - start_time), "다음작업중..")


#----> 원db 삭제하고 새로운 db붙여넣기
sql = "DROP TABLE elecgrdb"
cursor.execute(sql)

sql = "CREATE TABLE elecgrdb AS SELECT * FROM elecgrtempdb"
cursor.execute(sql)

print("%s초 소요됨. 원db삭제후 새로운 db 붙여넣기 작업완료. " % (time.time() - start_time), "모두완료!")

cursor.close()
aws_db.close()

