import pymysql
import time
import datetime
from multiprocessing import Pool
import concurrent.futures
import pandas as pd
import mysql.connector
import MySQLdb



#앱에서 카메라 범위 위경도를 받아와 위경도 범위내 법정동코드, 위경도, 정보를 AWS RDS에서 받아오는 함수
#AWS_Lambda_getbcode

start_time = time.time()
now_time = datetime.datetime.now()
print(now_time, "작업 시작")

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

"""
t_lat = event["queryStringParameters"]["t_lat"]
b_lat = event["queryStringParameters"]["b_lat"]
r_lng = event["queryStringParameters"]["r_lng"]
l_lng = event["queryStringParameters"]["l_lng"]
level = event["queryStringParameters"]["level"]
"""

t_lat = 37.606001
b_lat = 37.307641
r_lng = 126.700499
l_lng = 126.406166
level = 3

coordwlevel = [t_lat, b_lat, r_lng, l_lng, level]
cursor = aws_db.cursor()

sql = "SELECT bcode, arrt, lat, lng FROM elecgrcodedb WHERE lat <= %s AND lat >= %s AND lng <= %s AND lng >= %s AND level = %s"
cursor.execute(sql, coordwlevel)
result = cursor.fetchall()

cursor.close()
aws_db.close()

print("%s 초 소요됨" % (time.time() - start_time))

#--------------------------------------------->

#2
#위에서 추출한 카메라 구역내 법정동 지역(도,시,군,구,읍,명,동)의 충전가능 충전소 개수를 구하는 함수

host = ''
username = ''
password = ''
database = ''



'''
aws_db = pymysql.connect(
    host=host,  # DATABASE_HOST
    port=3306,
    user=username,  # DATABASE_USERNAME
    passwd=password,  # DATABASE_PASSWORD
    db=database,  # DATABASE_NAME
    charset='utf8'
)
'''

'''
def load_query(val):
    aws_db = pymysql.connect(host=host, port=3306, user=username, passwd=password, db=database, charset='utf8')
    cursor = aws_db.cursor()
    bcode = str(val[0])
    num = bcode[0:8]
    sbcode = num + "%"
    sql = "SELECT COUNT(*) FROM elecgrdb WHERE stat = 2 AND addcd LIKE %s"
    cursor.execute(sql, sbcode)
    res = cursor.fetchall()
    aws_db.close()
    return res[0][0]

with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    future_to_url = {executor.submit(load_query, val): val for val in result}
    for execution in concurrent.futures.as_completed(future_to_url):
        try:
            data = execution.result()
            print(data)

        except Exception as e:
            print("에러 : ", e)

#aws_db.close()

print("%s 초 소요됨" % (time.time() - start_time))
'''


'''
def work(num):
    db = MySQLdb.connect(host=host, port=3306, user=username, passwd=password, db=database, charset='utf8')
    cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) FROM elecgrdb WHERE stat = 2 AND addcd LIKE %s", (num + "%",))
    res = cursor.fetchall()
    print(res[0][0])
    print("%s 초 소요됨" % (time.time() - start_time))
    return None

executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

for val in result:
    bcode = str(val[0])
    num = bcode[0:8]
    executor.submit(work, num)
'''




aws_db = pymysql.connect(host=host, port=3306, user=username, passwd=password, db=database, charset='utf8')
#aws_db = MySQLdb.connect(host=host, port=3306, user=username, passwd=password, db=database, charset='utf8')

cursor = aws_db.cursor()
aws_db.commit()

array = []

for i, val in enumerate(result):
    bcode = str(val[0])
    num = bcode[0:8]
    sbcode = num + "%"
    sql = "SELECT COUNT(*) FROM elecgrdb WHERE stat = 2 AND addcd LIKE %s"
    cursor.execute(sql, sbcode)
    #cursor.execute("SELECT COUNT(*) FROM elecgrdb WHERE stat = 2 AND addcd LIKE %s", (num + "%",))
    res = cursor.fetchall()
    array.append(result[i]+res[0])

print(len(array))

cursor.close()
aws_db.close()

print("%s 초 소요됨" % (time.time() - start_time))


#    return {
#        'statusCode': 200,
#        'body': json.dumps(result)
#    }

