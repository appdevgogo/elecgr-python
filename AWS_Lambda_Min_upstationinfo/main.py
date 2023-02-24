import requests
from bs4 import BeautifulSoup
import pymysql
import time
import datetime
import configparser
from urllib import parse
import json
#import boto3

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

    #AWS Lambda 시행시 S3에서 임시로 파일을 받아서 시행하는 방법
    '''
    s3 = boto3.client('s3')
    s3.download_file("d-rawstationinfo", "config_210801_001.ini", "/tmp/config_210801_001.ini")

    config = configparser.ConfigParser()
    config.read('/tmp/config_210801_001.ini')
    '''

    #파이썬 로컬 파일에서 실행하는 방법

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
    tbname_update = "elecgr_info_update"
    tbname_check = "elecgr_info_check"
    period = 10

    check_update(tbname_check)

    aws_cursor.close()
    aws_db.close()
    print("***********업데이트 완료***********")

def check_update(tbname):

    sql = "SELECT ck, time FROM %s WHERE id=1" % (tbname)

    aws_cursor.execute(sql)
    result = aws_cursor.fetchall()

    sql_del = "TRUNCATE TABLE %s" % (tbname_update)
    aws_cursor.execute(sql_del)

    #del_table(dbname, tbname_update)
    #new_table(tbname_update)

    p_time = result[0][1]
    s_time = time.time()

    period = int((s_time-p_time)/60)+2
    print(period, "분전 업데이트 데이터 가져옴")

    check_up = result[0][0]

    if 1 == check_up:
        print("======>> 매분 충전소 업데이트 기본 시행")
        tbname_base = "elecgr_info"

    elif 0 == check_up:
        print("======>> 매분 충전소 업데이트 저장")
        tbname_base = "elecgr_info_save"

    up_stationinfo(tbname_base, tbname_update, period, check_up)
    up_time(s_time)  # 만약에 공공data에서 데이터를 가져오지 못하면 최근 업데이트 시간 갱신이 안됨
    print("--- 총 %s 초 소요됨. %d 분전까지 충전소 정보 업데이트 완료! ---" % ((time.time() - start_time), period))

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
          "statid VARCHAR(20) NOT NULL, " \
          "chgerid TINYINT NOT NULL, " \
          "stat TINYINT NOT NULL, " \
          "statupddt DATETIME NOT NULL," \
          "PRIMARY KEY (statid, chgerid)" \
          ")" % (tbname)
    aws_cursor.execute(sql)
    aws_db.commit()
    print("%s 테이블 생성함" % (tbname))

def up_stationinfo(tbname_base, tbname_update, period, check_up):
    pageno = 1
    numofrows = 9999
    params = "&pageNo=%d&numOfRows=%d&period=%d" % (pageno, numofrows, period)
    open_url = url + key + params

    res = requests.get(open_url)
    soup = BeautifulSoup(res.content, 'html.parser')
    totalcount = soup.find_all('totalcount')
    print(totalcount)

    data = soup.find_all('item')

    temp = []

    for item in data:
        statid = item.find('statid').text
        chgerid = item.find('chgerid').text
        stat = item.find('stat').text
        statupddt = item.find('statupddt').text
        lasttedt = item.find('lasttedt').text
        nowtsdt = item.find('nowtsdt').text
        temp.append([statid, chgerid, stat, statupddt, lasttedt, nowtsdt])
        print(statid, chgerid, stat, statupddt, lasttedt, nowtsdt)

    sql = "INSERT INTO elecgr_info_update (statid, chgerid, stat, statupddt, lasttedt, nowtsdt) VALUES (%s, %s, %s, %s, %s, %s)"
    aws_cursor.executemany(sql, temp)
    aws_db.commit()

    print("--- 총 %s 초 소요됨. 업데이트 임시테이블 생성 완료! ---" % (time.time() - start_time))


    if 1 == check_up:
        sql_s = "UPDATE %s A " \
                "LEFT JOIN %s B " \
                "ON A.statid = B.statid AND A.chgerid = B.chgerid " \
                "SET A.stat = B.stat, A.statupddt = B.statupddt, A.lasttedt = B.lasttedt, A.nowtsdt = B.nowtsdt " \
                "WHERE A.stat != B.stat OR A.statupddt != B.statupddt OR A.lasttedt != B.lasttedt OR A.nowtsdt != B.nowtsdt" % (tbname_base, tbname_update)

        aws_cursor.execute(sql_s)
        aws_db.commit()

    if 0 == check_up:
        sql_s = "UPDATE %s A " \
                "LEFT JOIN %s B " \
                "ON A.statid = B.statid AND A.chgerid = B.chgerid " \
                "SET A.stat = B.stat, A.statupddt = B.statupddt, A.lasttedt = B.lasttedt, A.nowtsdt = B.nowtsdt " \
                "WHERE A.stat != B.stat OR A.statupddt != B.statupddt OR A.lasttedt != B.lasttedt OR A.nowtsdt != B.nowtsdt" % (tbname_base, tbname_update)

        aws_cursor.execute(sql_s)
        aws_db.commit()

        sql_s = "INSERT IGNORE INTO %s (statid, chgerid, stat, statupddt, lasttedt, nowtsdt) " \
                "SELECT statid, chgerid, stat, statupddt, lasttedt, nowtsdt FROM %s" % (tbname_base, tbname_update)

        aws_cursor.execute(sql_s)
        aws_db.commit()

    '''
    sql_s = "CREATE TABLE elecgr_info_final " \
            "SELECT * FROM elecgr_info " \
            "LEFT JOIN elecgr_info_update " \
            "ON elecgr_info.statid = elecgr_info_update.statidup AND elecgr_info.chgerid = elecgr_info_update.chgeridup"

    aws_cursor.execute(sql_s)
    aws_db.commit()
    '''

    '''
    sql = "UPDATE elecgr_info SET stat=%s, statupddt=%s WHERE statid=%s AND chgerid=%s"
    aws_cursor.executemany(sql, temp)
    aws_db.commit()
    '''

    return None

def up_time(timeto):
    sql = "UPDATE elecgr_info_check SET time=%s" % (timeto)
    aws_cursor.execute(sql)
    aws_db.commit()

init()




