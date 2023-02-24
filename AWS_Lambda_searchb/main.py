import pymysql
import json
import time
import datetime
#import boto3

def lambda_handler(event, context):

    #txt_first = event["queryStringParameters"]["t_first"]
    #txt_second = event["queryStringParameters"]["t_second"]
    #txt_third = event["queryStringParameters"]["t_third"]
    #txt_fourth = event["queryStringParameters"]["t_fourth"]
    #txt_fifth = event["queryStringParameters"]["t_fifth"]

    txt_first = "경기"
    txt_second = "수원"
    txt_third = "권선"
    txt_fourth = ""
    txt_fifth = ""

    print("시작됨")

    send = init(txt_first, txt_second, txt_third, txt_fourth, txt_fifth)

    return {
        'statusCode': 200,
        'body': json.dumps(send)
    }

def init(txt_first, txt_second, txt_third, txt_fourth, txt_fifth):

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

    text = ["%" + txt_first + "%", "%" + txt_second + "%", "%" + txt_third + "%", "%" + txt_fourth + "%",
            "%" + txt_fifth + "%"]

    sql = "SELECT arr, lat, lng FROM elecgr_bcode WHERE arr LIKE %s AND arr LIKE %s AND arr LIKE %s AND arr LIKE %s AND arr LIKE %s LIMIT 3"

    aws_cursor.execute(sql, text)
    result = aws_cursor.fetchall()

    aws_cursor.close()
    aws_db.close()

    return result

