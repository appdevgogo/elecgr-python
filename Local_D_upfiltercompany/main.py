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
    config.read('..\config\evspot_config_220808.ini')

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
    tbname = "elecgr_filter_com"


    del_table(dbname, tbname)
    new_table(tbname)
    insert_filter_company()
    update_filter_company()

    aws_cursor.close()
    aws_db.close()

    print("--- 총 %s 초 소요됨. 작업완료! ---" % (time.time() - start_time))

def del_table(dbname, tbname):
    # 해당 이름의 테이블이 있는지 없는지 확인하는 것
    sql = "SELECT count(*) FROM Information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'" % (dbname, tbname)

    aws_cursor.execute(sql)
    result = aws_cursor.fetchall()

    if 1 == result[0][0]:
        sql = "DROP TABLE %s" % (tbname)
        aws_cursor.execute(sql)
        print("%s 테이블 삭제함" % (tbname))


def new_table(tbname):
    sql = "CREATE TABLE %s " \
          "(" \
          "busiid VARCHAR(10) NOT NULL, " \
          "businm VARCHAR(100) NOT NULL, " \
          "PRIMARY KEY (busiid)" \
          ")" % (tbname)
    aws_cursor.execute(sql)
    aws_db.commit()
    print("%s 테이블 생성함" % (tbname))


def insert_filter_company():

    sql_s = "SELECT busiid, businm FROM elecgr_info GROUP BY busiid"

    aws_cursor.execute(sql_s)
    aws_db.commit()

    result = aws_cursor.fetchall()

    print(result)
    print(len(result))

    sql_s = "INSERT INTO elecgr_filter_com (busiid, businm) VALUES (%s, %s)"
    aws_cursor.executemany(sql_s, result)
    aws_db.commit()

    '''
    VALUES (%s)여기세어 %s 요 형태로 들어가야함! 중요!
    '''

def update_filter_company():

    temp_a = []

    temp = (('BA', '부안군'), ('BN', '블루네트웍스'), ('BT', '보타리에너지'),
            ('CU', '씨어스'), ('CV', '대영채비'), ('DE', '대구환경공단'), ('DG', '대구환경공단'),
            ('EK', '이노케이텍'), ('EM', 'evmost'), ('EN', '이엔'), ('EP', '이카플러그'), ('EV', '에버온'), ('EZ', '차지인'),
            ('G2', '광주광역시'), ('GN', '지커넥트'), ('GP', '군포시'), ('GS', 'GS칼텍스'),
            ('HD', '하이차저/E-pit'), ('HE', '한국전기차충전서비스'), ('HL', '스포컴'), ('HM', 'HUMAX EV'), ('IK', '익산시'),
            ('JC', '제주에너지공사'), ('JD', '제주특별자치도'), ('JE', '제주전기자동차서비스'), ('JJ', '전주시'), ('JT', '제주테크노파크'), ('JU', '정읍시'),
            ('KC', '한국컴퓨터'), ('KE', '한국전기차인프라기술'), ('KL', '클린일렉스'), ('KN', '한국환경공단'), ('KP', '한국전력공사'),
            ('LH', 'LG헬로비전'), ('ME', '환경부'), ('MO', '매니지온'), ('NJ', '나주시'), ('OB', '현대오일뱅크'),
            ('PI', '차지비'), ('PW', '파워큐브'), ('RE', '레드이엔지'),
            ('SC', '삼척시'), ('SE', '서울특별시'), ('SF', '스타코프'), ('SG', '시그넷'), ('SJ', '세종특별자치시'), ('SK', 'SK에너지'),
            ('SM', '성민기업'), ('SN', '서울에너지공사'), ('SS', '삼성이브이씨'), ('ST', '에스트래픽'),
            ('TB', '태백시'), ('TD', '타디스테크놀로지'), ('US', '울산광역시'), ('YY', '양양군'))

    for item in temp:
        print(item[1], item[0])
        temp_a.append((item[1], item[0]))

    print(temp_a)

    sql_s = "UPDATE elecgr_filter_com SET businm = %s WHERE busiid = %s"
    aws_cursor.executemany(sql_s, temp_a)
    aws_db.commit()

    '''
    VALUES (%s)여기세어 %s 요 형태로 들어가야함! 중요!
    '''


init()