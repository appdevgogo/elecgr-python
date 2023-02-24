import pymysql
import time
import datetime
import configparser
from urllib import parse
import json


# import boto3

def lambda_handler(event, context):
    init()

    return {
        'statusCode': 200,
        'body': json.dumps('ok')
    }


def init():
    global start_time, now_time, url, key, \
        aws_host, aws_user, aws_pw, aws_dbname, aws_db, aws_cursor, \
        dbname, tbname_update, tbname_check, period

    start_time = time.time()
    now_time = datetime.datetime.now()
    print(now_time, "작업 시작")

    # AWS System Manager(SMM) 에서 key parameter를 할용해서 하는 방법
    '''
    ssmm = boto3.client('ssm')

    url_p = ssmm.get_parameter(Name='/elecgr/config/datagokr/endpointst', WithDecryption=True)
    key_p = ssmm.get_parameter(Name='/elecgr/config/datagokr/key', WithDecryption=True)

    aws_host_p = ssmm.get_parameter(Name='/elecgr/config/awsrds/host', WithDecryption=True)
    aws_user_p = ssmm.get_parameter(Name='/elecgr/config/awsrds/user', WithDecryption=True)
    aws_pw_p = ssmm.get_parameter(Name='/elecgr/config/awsrds/pw', WithDecryption=True) #pw encoding값으로 바로 사용
    aws_dbname_p = ssmm.get_parameter(Name='/elecgr/config/awsrds/db', WithDecryption=True)

    url = url_p['Parameter']['Value']
    key = key_p['Parameter']['Value']

    aws_host = aws_host_p['Parameter']['Value']
    aws_user = aws_user_p['Parameter']['Value']
    aws_pw = aws_pw_p['Parameter']['Value']
    aws_dbname = aws_dbname_p['Parameter']['Value']
    '''

    # AWS Lambda 시행시 S3에서 임시로 파일을 받아서 시행하는 방법
    '''
    s3 = boto3.client('s3')
    s3.download_file("d-rawstationinfo", "config_210801_001.ini", "/tmp/config_210801_001.ini")

    config = configparser.ConfigParser()
    config.read('/tmp/config_210801_001.ini')
    '''

    # 파이썬 로컬 파일에서 실행하는 방법

    config = configparser.ConfigParser()
    config.read('..\AWS_Lambda_D_rawstationinfo\config_210801_001.ini')

    url = config['DATAGOKR']['ENDPOINTST']
    key_d = config['DATAGOKR']['KEY']
    key = parse.quote(key_d, safe='')  # s3 config.ini 에 저장된 decoding key값을 encoding 하는 문장

    aws_host = config['AWSRDS']['HOST']
    aws_user = config['AWSRDS']['USER']
    aws_pw = config['AWSRDS']['PW']
    aws_dbname = config['AWSRDS']['DB']

    aws_db = pymysql.connect(
        host=aws_host,
        port=3306,
        user=aws_user,
        password=aws_pw,
        database=aws_dbname,
        charset='utf8'
    )

    aws_cursor = aws_db.cursor()

    dbname = "elecgr_info"
    get_filter_company()

    aws_cursor.close()
    aws_db.close()

    print("--- 총 %s 초 소요됨. 작업완료! ---" % (time.time() - start_time))


def get_filter_company():

    sql_s = "SELECT busiid, businm FROM elecgr_filter_com ORDER BY businm ASC"

    '''sql_s = "SELECT DISTINCT busiid, businm  FROM elecgr_info"'''


    aws_cursor.execute(sql_s)
    aws_db.commit()

    result = aws_cursor.fetchall()

    print(result)
    print(len(result))

    return result


init() #Lambda에 넣을때는 삭제해야함