import pymysql
import json
import time
import datetime
#import boto3


#searchc 이지만 실제로 search a로 사용됨. 현재 지도에서 제일 가까운 것을 선정
def lambda_handler(event, context):

    #txt = event["queryStringParameters"]["txt"]
    #lat_c = event["queryStringParameters"]["latc"]
    #lng_c = event["queryStringParameters"]["lngc"]
    txt = "종"
    lat_c = "37.571076"
    lng_c = "126.99588"

    send = init(txt, lat_c, lng_c)

    return {
        'statusCode': 200,
        'body': json.dumps(send)
    }


def init(txt, lat_c, lng_c):

    global start_time, now_time, \
        aws_host, aws_user, aws_pw, aws_dbname, aws_db, aws_cursor

    start_time = time.time()
    now_time = datetime.datetime.now()
    print(now_time, "작업 시작")

    aws_host = ''
    aws_user = ''
    aws_pw = ''
    aws_dbname = ''

    aws_db = pymysql.connect(
        host=aws_host,
        port=3306,
        user=aws_user,
        password=aws_pw,
        database=aws_dbname,
        charset='utf8'
    )

    aws_cursor = aws_db.cursor()

    txt_q = "%" + txt + "%"

    '''
    sql = "SELECT statid, statnm, addr FROM elecgrdb WHERE statnm LIKE %s LIMIT 5"
    cursor.execute(sql, txt_q)
    result = cursor.fetchall()
    '''

    sql = "SELECT statid, statnm, addr, lat, lng FROM elecgr_info WHERE statnm LIKE %s GROUP BY statid LIMIT 5"
    aws_cursor.execute(sql, txt_q)
    result = aws_cursor.fetchall()

    temp = []

    for i, v in enumerate(result):
        dif = abs((float(lat_c) - v[3])) + abs((float(lng_c) - v[4]))
        temp.append([i, dif])

    temp.sort(key=lambda x: x[1])

    send = []
    for val in temp[0:5]:
        send.append(result[val[0]])

    aws_cursor.close()
    aws_db.close()

    return send


