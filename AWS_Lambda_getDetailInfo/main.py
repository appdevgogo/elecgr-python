import pymysql
import time
import datetime
import configparser
import json
#import boto3


# 카메라 범위내 충전소 id, 위경도 정보를 가져오는 함수(마커생성용)
# AWS_Lambda_getstationinfo
# AWS상에는 getMarkersInfo 이름으로 사용


def lambda_handler(event, context):
    statid = event["queryStringParameters"]["stid"]

    return_data = init(statid)

    return {
        'statusCode': 200,
        'body': json.dumps(return_data)
    }


def init(statid):
    start_time = time.time()
    now_time = datetime.datetime.now()
    print(now_time, "작업 시작")

    # AWS System Manager(SMM) 에서 key parameter를 할용해서 하는 방법(그러나 속도가 너무 늦어짐)
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

    '''
    config = configparser.ConfigParser()
    config.read('../config/evspot_config_220808.ini')

    aws_host = config['AWSRDS']['HOST']
    aws_user = config['AWSRDS']['USER']
    aws_pw = config['AWSRDS']['PW']
    aws_dbname = config['AWSRDS']['DB']

    aws_db = pymysql.connect(
        host=aws_host,
        port=3306,
        user=aws_user,
        passwd=aws_pw,
        db=aws_dbname,
        charset='utf8',
        cursorclass=pymysql.cursors.DictCursor
    )

    coord = [statid]
    cursor = aws_db.cursor()

    sql = "SELECT * FROM elecgr_info WHERE statid = %s"
    cursor.execute(sql, coord)
    result = cursor.fetchall()

    print(result)
    print(len(result))

    cursor.close()
    aws_db.close()

    print("--- 총 %s 초 소요됨. 작업완료! ---" % (time.time() - start_time))


init("BNAA0101")