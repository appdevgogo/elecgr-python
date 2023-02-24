import requests
from bs4 import BeautifulSoup
import pymysql
import sqlite3
import concurrent.futures
import time
import datetime
import base64
import boto3
import configparser
from urllib import parse

# ---------->> 공공데이터에서 받은 충전소 위경도로 네이버 API활용하여 법정동 코드 추출후 AWS에 저장
# (이것이 메인임 sqlite 활용해서 사용하는 것은 아직 업데이트 동안 충전기 상태 변경된 것이 반영 안됨)

def init():

    global start_time, url, key, \
        aws_host, aws_user, aws_pw, aws_dbname, aws_db, aws_cursor, \
        clientid, clientsecret

    start_time = time.time()
    print(datetime.datetime.now(), "작업시작", "진행중..")


    #AWS Lambda 시행시 S3에서 임시로 파일을 받아서 시행하는 방법
    '''
    s3 = boto3.client('s3')
    s3.download_file("d-rawstationinfo", "config_210801_001.ini", "/tmp/config_210801_001.ini")

    config = configparser.ConfigParser()
    config.read('/tmp/config_210801_001.ini')
    '''

    # AWS System Manager(SMM) 에서 key parameter를 할용해서 하는 방법
    '''
    ssmm = boto3.client('ssm', region_name='ap-northeast-2')

    url_p = ssmm.get_parameter(Name='/elecgr/config/datagokr/endpoint', WithDecryption=True)
    key_p = ssmm.get_parameter(Name='/elecgr/config/datagokr/key', WithDecryption=True)
    
    clientid_p = ssmm.get_parameter(Name='/elecgr/config/navergeo/clientid', WithDecryption=True)
    clientsecret_p = ssmm.get_parameter(Name='/elecgr/config/navergeo/clientsecret', WithDecryption=True)

    aws_host_p = ssmm.get_parameter(Name='/elecgr/config/awsrds/host', WithDecryption=True)
    aws_user_p = ssmm.get_parameter(Name='/elecgr/config/awsrds/user', WithDecryption=True)
    aws_pw_p = ssmm.get_parameter(Name='/elecgr/config/awsrds/pw', WithDecryption=True) #pw encoding값으로 바로 사용
    aws_dbname_p = ssmm.get_parameter(Name='/elecgr/config/awsrds/db', WithDecryption=True)
    

    url = url_p['Parameter']['Value']
    key = key_p['Parameter']['Value']
    
    clientid = clientid_p['Parameter']['Value']
    clientsecret = clientsecret_p['Parameter']['Value']

    aws_host = aws_host_p['Parameter']['Value']
    aws_user = aws_user_p['Parameter']['Value']
    aws_pw = aws_pw_p['Parameter']['Value']
    aws_dbname = aws_dbname_p['Parameter']['Value']
    
    '''

    ''''''
    config = configparser.ConfigParser()
    config.read('..\config\evspot_config_220808.ini')

    url = config['DATAGOKR']['ENDPOINT']
    key_d = config['DATAGOKR']['KEY']
    key = parse.quote(key_d, safe='')  # s3 config.ini 에 저장된 decoding key값을 encoding 하는 문장

    clientid = config['NAVERGEO']['CLIENTID']
    clientsecret = config['NAVERGEO']['CLIENTSECRET']

    aws_host = config['AWSRDS']['HOST']
    aws_user = config['AWSRDS']['USER']
    aws_pw = config['AWSRDS']['PW']
    aws_dbname = config['AWSRDS']['DB']
    ''''''

    aws_db = pymysql.connect(
        host=aws_host,
        port=3306,
        user=aws_user,
        passwd=aws_pw,
        db=aws_dbname,
        charset='utf8'
    )

    aws_cursor = aws_db.cursor()

    step_zero()
    step_one()

    #step_two()
    #step_three()
    #step_four()
    step_five()

    aws_cursor.close()
    aws_db.close()

    #stop_ec2()

    print("--- 누적 %s 초 소요됨. ---" % (time.time() - start_time), "작업 종료!!!!")

#초기 필요한 table들 생성
def step_zero():

    if table_exit("elecgr_info", "elecgr_info_save") == 0:
        sql = "CREATE TABLE elecgr_info_save " \
              "(" \
              "statid VARCHAR(20) NOT NULL, " \
              "chgerid TINYINT NOT NULL, " \
              "stat TINYINT NOT NULL, " \
              "statupddt DATETIME NOT NULL, " \
              "lasttedt DATETIME NOT NULL, " \
              "nowtsdt DATETIME NOT NULL, " \
              "PRIMARY KEY (statid, chgerid)" \
              ")"

        aws_cursor.execute(sql)

        print("elecgr_info_save 테이블 생성함")

    if table_exit("elecgr_info", "elecgr_info_update") == 0:
        sql = "CREATE TABLE elecgr_info_update " \
              "(" \
              "statid VARCHAR(20) NOT NULL, " \
              "chgerid TINYINT NOT NULL, " \
              "stat TINYINT NOT NULL, " \
              "statupddt DATETIME NOT NULL, " \
              "lasttedt DATETIME NOT NULL, " \
              "nowtsdt DATETIME NOT NULL, " \
              "PRIMARY KEY (statid, chgerid)" \
              ")"

        aws_cursor.execute(sql)

        print("elecgr_info_update 테이블 생성함")

    if table_exit("elecgr_info", "elecgr_info_check") == 0:
        sql = "CREATE TABLE elecgr_info_check " \
              "(" \
              "id INT(10) NOT NULL, " \
              "ck INT(10) NOT NULL, " \
              "time DOUBLE NOT NULL, " \
              "PRIMARY KEY (id)" \
              ")"

        aws_cursor.execute(sql)

        temp = [(1, 1, time.time())]

        sql = "INSERT INTO elecgr_info_check " \
              "(id, ck, time) " \
              "VALUES " \
              "(%s, %s, %s)"

        aws_cursor.executemany(sql, temp)
        aws_db.commit()

        print("elecgr_info_check 테이블 생성함")


# ---------->> 1단계 : 공공데이터를 받아서 aws rds파일에 저장하는 작업
def step_one():

    s_time = time.time()

    sql_del = "TRUNCATE TABLE %s" % ("elecgr_info_save")
    aws_cursor.execute(sql_del)

    up_status(0)
    del_table("elecgr_info", "elecgr_info")

    sql = "CREATE TABLE elecgr_info " \
          "(" \
          "statnm VARCHAR(150) NOT NULL, " \
          "statid VARCHAR(20) NOT NULL, " \
          "chgerid TINYINT NOT NULL, " \
          "chgertype TINYINT NOT NULL, " \
          "addr VARCHAR(300) NOT NULL, " \
          "location VARCHAR(300) NOT NULL, " \
          "lat DOUBLE NOT NULL," \
          "lng DOUBLE NOT NULL, " \
          "addcd VARCHAR(20) NOT NULL, " \
          "usetime VARCHAR(150) NOT NULL, " \
          "busiid VARCHAR(10) NOT NULL, " \
          "bnm VARCHAR(100) NOT NULL, " \
          "businm VARCHAR(100) NOT NULL, " \
          "busicall VARCHAR(50) NOT NULL, " \
          "stat TINYINT NOT NULL, " \
          "statupddt DATETIME NOT NULL, " \
          "lasttsdt DATETIME NOT NULL, " \
          "lasttedt DATETIME NOT NULL, " \
          "nowtsdt DATETIME NOT NULL, " \
          "output INT NOT NULL, " \
          "method VARCHAR(50) NOT NULL, " \
          "zcode TINYINT NOT NULL, " \
          "parkingfree VARCHAR(10) NOT NULL, " \
          "note VARCHAR(400) NOT NULL, " \
          "limityn VARCHAR(10) NOT NULL, " \
          "limitdetail VARCHAR(150) NOT NULL, " \
          "delyn VARCHAR(10) NOT NULL, " \
          "deldetail VARCHAR(150) NOT NULL," \
          "PRIMARY KEY (statid, chgerid)" \
          ")"

    aws_cursor.execute(sql)

    print("elecgr_info 테이블 생성함")

    pageno = 1
    inumofrow = 100
    numofrows = 9999 #1페이지에 9999개 이상이 나오게 되면 이때 문제가 되겠다.

    params = "&pageNo=%d&numOfRows=%d" % (pageno, inumofrow)

    open_url = url + key + params
    res = requests.get(open_url)
    soup = BeautifulSoup(res.content, 'html.parser')  # 'xlml 이나 html.parser 이나 별차이 없음
    totalcount = soup.find_all('totalcount')

    print(totalcount)

    # 만약 충전기개수가 75,000개 이면 총 8페이지가 발생됨 왜냐하면 1페이지당 9,999개까지만 추출 가능
    # 파이썬 range(1, 3) 이면 1,2만 실행됨
    count = int(int(totalcount[0].text) / numofrows) + 2

    for i in range(1, count):
        print(i)
        pageno = i
        params = "&pageNo=%d&numOfRows=%d&" % (pageno, numofrows)
        open_url = url + key + params

        res = requests.get(open_url)
        soup = BeautifulSoup(res.content, 'html.parser')
        data = soup.find_all('item')

        print("------> sub1")

        temp = []
        for item in data:
            statnm = item.find('statnm').text
            statid = item.find('statid').text
            chgerid = item.find('chgerid').text
            chgertype = item.find('chgertype').text
            addr = item.find('addr').text
            location = item.find('location').text
            lat = item.find('lat').text
            lng = item.find('lng').text
            usetime = item.find('usetime').text
            busiid = item.find('busiid').text
            bnm = item.find('bnm').text
            businm = item.find('businm').text
            busicall = item.find('busicall').text
            stat = item.find('stat').text
            statupddt = item.find('statupddt').text
            lasttsdt = item.find('lasttsdt').text
            lasttedt = item.find('lasttedt').text
            nowtsdt = item.find('nowtsdt').text
            output = item.find('output').text
            method = item.find('method').text
            zcode = item.find('zcode').text
            parkingfree = item.find('parkingfree').text
            note = item.find('note').text
            limityn = item.find('limityn').text
            limitdetail = item.find('limitdetail').text
            delyn = item.find('delyn').text
            deldetail = item.find('deldetail').text

            temp.append([statnm, statid, chgerid, chgertype, addr, location, lat, lng, usetime,
                         busiid, bnm, businm, busicall, stat, statupddt, lasttsdt, lasttedt, nowtsdt,
                         output, method, zcode, parkingfree, note, limityn, limitdetail, delyn, deldetail])

        print("------> sub2")

        sql = "INSERT INTO elecgr_info " \
              "(statnm, statid, chgerid, chgertype, addr, location, lat, lng, usetime, " \
              "busiid, bnm, businm, busicall, stat, statupddt, lasttsdt, lasttedt, nowtsdt, " \
              "output, method, zcode, parkingfree, note, limityn, limitdetail, delyn, deldetail) " \
              "VALUES " \
              "(%s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s)"

        aws_cursor.executemany(sql, temp)
        aws_db.commit()
        print("------> sub3")

    up_time(s_time)
    up_status(1)

    print("--- 1단계 공공데이터 추출후 테이블 생성 %s 초 소요됨. 작업완료! ---" % (time.time() - start_time))
    print("--- 누적 %s 초 소요됨. ---" % (time.time() - start_time), "다음작업 진행중..")

# ---------->> 2단계 : 위경도 법정동 코드를 네이버로 추출하여 AWS RDS에 저장 작업
def step_two():

    start_time_step = time.time()
    sql = "SELECT statid, lat, lng FROM elecgr_info"
    aws_cursor.execute(sql)
    result = aws_cursor.fetchall()

    geoURLS = []
    temp = []

    for item in result:
        statid = item[0]
        geocode = str(item[2]) + "," + str(item[1])
        furl = "" % (
        statid,
        clientid, clientsecret, geocode)
        geoURLS.append(furl)

    geoURLS_temp = set(geoURLS)
    geoURLS = list(geoURLS_temp)

    def load_url(furl):
        url_r = furl[8:]
        res = requests.get(url_r)
        soup = BeautifulSoup(res.content, 'html.parser')
        code = soup.find('id').text
        return code

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(load_url, furl): furl for furl in geoURLS}
        for future in concurrent.futures.as_completed(future_to_url):
            furl = future_to_url[future]
            try:
                data = future.result()
                print(data, furl[:8])
                temp.append([data, furl[:8]])

            except Exception as e:
                print("0", furl[:8])
                temp.append(["0", furl[:8]])

    print("--- 2-1단계 %s 초 소요됨. 법정동코드 네이버 RNGEO에서 추출완료 ---" % (time.time() - start_time_step))
    print("--- 누적 %s 초 소요됨. ---" % (time.time() - start_time), "다음작업 진행중..")

    start_time_step = time.time()

    del_table("elecgr_info", "elecgr_info_addcd")

    sql = "CREATE TABLE elecgr_info_addcd (" \
          "addcd VARCHAR(20) NOT NULL, " \
          "statid VARCHAR(20) NOT NULL, " \
          "PRIMARY KEY(statid))"

    aws_cursor.execute(sql)
    print("elecgr_info_addcd 테이블 생성함")

    sql = "INSERT IGNORE INTO elecgr_info_addcd (addcd, statid) VALUES (%s, %s)"

    aws_cursor.executemany(sql, temp)
    aws_db.commit()

    print("--- 2-2단계 작업 %s 초 소요됨. 작업완료! ---" % (time.time() - start_time_step))
    print("--- 누적 %s 초 소요됨. ---" % (time.time() - start_time), "다음작업 진행중..")

    start_time_step = time.time()

    sql_c = "UPDATE elecgr_info A " \
            "LEFT JOIN elecgr_info_addcd B " \
            "ON A.statid = B.statid " \
            "SET A.addcd = B.addcd "

    '''
    del_table("elecgr_info", "elecgr_info")
    
    sql_c = "CREATE TABLE elecgr_info " \
            "SELECT statnm, elecgr_info_gokr.statid, chgerid, chgertype, addr, elecgr_info_addcd.addcd, lat, lng, usetime, " \
                    "busiid, businm, busicall, stat, statupddt, powertype, output, method, " \
                    "zcode, parkingfree, note, limityn, limitdetail, delyn, deldetail " \
            "FROM elecgr_info_gokr " \
            "LEFT JOIN elecgr_info_addcd " \
            "ON elecgr_info_gokr.statid = elecgr_info_addcd.statid"
    '''

    aws_cursor.execute(sql_c)
    aws_db.commit()

    print("--- 2-3단계 elecgr_info 테이블 생성(LEFT JOIN) %s 초 소요됨. 작업완료! ---" % (time.time() - start_time_step))
    print("--- 누적 %s 초 소요됨. ---" % (time.time() - start_time), "다음작업 진행중..")

# ---------->> 3번째 : 법정동 코드 0 또는 NULL값 위경도 수정 작업
def step_three():

    start_time_step = time.time()
    sql = "SELECT statid, addr FROM elecgr_info WHERE addcd = '0' OR addcd IS NULL"  # LIMIT 0, 100 / WHERE addcd IS NULL
    aws_cursor.execute(sql)
    result = aws_cursor.fetchall()

    geoURLS = []
    temp = []

    for item in result:
        statid = item[0]
        add = item[1]
        furl = "" % (
        statid,
        clientid, clientsecret, add)
        geoURLS.append(furl)

    geoURLS_temp = set(geoURLS)
    geoURLS = list(geoURLS_temp)

    def load_url(furl):
        url_r = furl[8:]
        res = requests.get(url_r).json()
        lat = res['addresses'][0]['y']
        lng = res['addresses'][0]['x']
        return lat, lng

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(load_url, furl): furl for furl in geoURLS}
        for future in concurrent.futures.as_completed(future_to_url):
            furl = future_to_url[future]
            try:
                data = future.result()
                print(data, furl[:8])
                temp.append([data[0], data[1], furl[:8]])

            except Exception as e:
                print("3-1단계 작업 에러 발생")

    print("--- 3-1단계 작업 %s 초 소요됨. 법정동코드 0 또는 NULL값 NGEO에서 위경도 추출작업 완료! ---" % (time.time() - start_time_step))
    print("--- 누적 %s 초 소요됨. ---" % (time.time() - start_time), "다음작업 진행중..")

    start_time_step = time.time()

    aws_cursor.executemany("UPDATE elecgr_info SET lat = %s, lng = %s WHERE statid = %s", temp)
    aws_db.commit()

    print("--- 3-2단계 작업 %s 초 소요됨. 법정동코드 0 또는 NULL값 주출된 위경도 업데이트 완료! ---" % (time.time() - start_time_step))
    print("--- 누적 %s 초 소요됨. ---" % (time.time() - start_time), "다음작업 진행중..")

# ---------->> 4번째 : 법정동코드 0 또는 NULL값 수정된 충전소 위경도 기준으로 법정동 재추출 작업
def step_four():

    start_time_step = time.time()

    sql = "SELECT statid, lat, lng FROM elecgr_info WHERE addcd = '0' OR addcd IS NULL"  # LIMIT 0, 100 / WHERE addCd IS NULL
    aws_cursor.execute(sql)
    result = aws_cursor.fetchall()

    geoURLS = []
    temp = []

    for item in result:
        statid = item[0]
        geocode = str(item[2]) + "," + str(item[1])
        furl = "" % (
        statid,
        clientid, clientsecret, geocode)
        geoURLS.append(furl)

    geoURLS_temp = set(geoURLS)
    geoURLS = list(geoURLS_temp)

    def load_url(furl):
        url_r = furl[8:]
        res = requests.get(url_r)
        soup = BeautifulSoup(res.content, 'html.parser')
        code = soup.find('id').text
        return code

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(load_url, furl): furl for furl in geoURLS}
        for future in concurrent.futures.as_completed(future_to_url):
            furl = future_to_url[future]
            try:
                data = future.result()
                print(data, furl[:8])
                temp.append([data, furl[:8]])

            except Exception as e:
                print("0", furl[:8])
                temp.append(["0", furl[:8]])

    print("--- 4-1단계 작업 %s 초 소요됨. 법정동코드 0 또는 NULL값 수정된 충전소 위경도로 RNGEO에서 법정동 재추출 완료! ---" % (
                time.time() - start_time_step))
    print("--- 누적 %s 초 소요됨. ---" % (time.time() - start_time), "다음작업 진행중..")

    start_time_step = time.time()

    aws_cursor.executemany("UPDATE elecgr_info SET addcd = %s WHERE statid = %s", temp)
    aws_db.commit()

    print("--- 4-2단계 작업 %s 초 소요됨. 법정동코드 0 또는 NULL값 수정된 충전소 위경도 기준으로 법정동 업데이트 완료! ---" % (time.time() - start_time_step))
    print("--- 누적 %s 초 소요됨. ---" % (time.time() - start_time), "다음작업 진행중..")

# ---------->> 5번째 : elecgr-info table 업데이트 동안 누적된 충전소 상태들 최신 업데이트
def step_five():

    start_time_step = time.time()

    sql_s = "UPDATE elecgr_info A " \
            "LEFT JOIN elecgr_info_save B " \
            "ON A.statid = B.statid AND A.chgerid = B.chgerid " \
            "SET A.stat = B.stat, A.statupddt = B.statupddt " \
            "WHERE A.stat != B.stat OR A.statupddt != B.statupddt"

    aws_cursor.execute(sql_s)
    aws_db.commit()

    print("--- 5단계 작업 %s 초 소요됨. elecgr-info table 업데이트 동안 누적된 충전소 상태들 최신 업데이트 완료! ---" % (time.time() - start_time_step))
    print("--- 누적 %s 초 소요됨. ---" % (time.time() - start_time), "다음작업 진행중..")

# 테이블 삭제 함수
def del_table(dbname, tablename):
    # 해당 이름의 테이블이 있는지 없는지 확인하는 것
    sql = "SELECT count(*) FROM Information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'" % (dbname, tablename)

    aws_cursor.execute(sql)
    result = aws_cursor.fetchall()

    if 1 == result[0][0]:
        sql = "DROP TABLE %s" % (tablename)
        aws_cursor.execute(sql)
        print("%s 테이블 삭제함" % (tablename))

# 해당 이름의 테이블이 있는지 없는지 확인하는 것
def table_exit(dbname, tablename):
    sql = "SELECT count(*) FROM Information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'" % (dbname, tablename)
    aws_cursor.execute(sql)
    result = aws_cursor.fetchall()

    return result[0][0]


# 최근 충전소 정보 업데이트 시간
def up_time(timeto):
    sql = "UPDATE elecgr_info_check SET time=%s WHERE id=1" % (timeto)
    aws_cursor.execute(sql)
    aws_db.commit()

# 충전소 check 정보 업로드
def up_status(yn):

    sql = "UPDATE elecgr_info_check SET ck=%s WHERE id=1" % (yn) #왜그런지 모르겠지만 컬럼명을 check로 하면 안됨
    aws_cursor.execute(sql)
    aws_db.commit()


init()


'''
# ---------->> 5번째 : 위경도 겹치는 충전소 다시 받아서 위경도 수정하는 작업 
# 너무 시간이 오래 걸리므로 해당 작업은 Mobile Phone 에서 직접구현 필요
start_time_step = time.time()
sql = "SELECT statid, lat, lng FROM elecgr_info GROUP BY statid"  # LIMIT 0, 100 / WHERE addcd IS NULL
aws_cursor.execute(sql)
result = aws_cursor.fetchall()

interval = 0.00005 #위도(lat) 1도가 대략 111km, 경도(lng) 1도가 대략 89km, 평균 100km 잡음
ds = 0.0001
temp = []
temp_id = []

def modifylatlng(result):
    c = len(result)

    # 파이썬 range(1, 3) 이면 1,2만 실행됨
    for i in range(0, c):

        if i == 0:
            new_lat = result[i][0] + ds
            new_lng = result[i][1]

        elif i == 1:
            new_lat = result[i][0] - ds
            new_lng = result[i][1]

        elif i == 2:
            new_lat = result[i][0]
            new_lng = result[i][1] + ds

        elif i == 3:
            new_lat = result[i][0]
            new_lng = result[i][1] - ds

        elif i == 4:
            new_lat = result[i][0] + ds
            new_lng = result[i][1] + ds

        elif i == 5:
            new_lat = result[i][0] - ds
            new_lng = result[i][1] - ds

        elif i == 6:
            new_lat = result[i][0] + ds
            new_lng = result[i][1] - ds

        elif i == 7:
            new_lat = result[i][0] - ds
            new_lng = result[i][1] + ds

        temp.append([new_lat, new_lng, result[i][2]])
        temp_id.append(result[i][2])

    return None


for val in result:
    stid_ori = val[0]
    t_lat = val[1] + interval
    b_lat = val[1] - interval
    r_lng = val[2] + interval
    l_lng = val[2] - interval
    sql = "SELECT lat, lng, statid FROM elecgr_info " \
          "WHERE NOT statid = %s " \
          "AND lat <= %s AND lat >= %s AND lng <= %s AND lng >= %s GROUP BY statid"
    aws_cursor.execute(sql, (stid_ori, t_lat, b_lat, r_lng, l_lng))
    result = aws_cursor.fetchall()

    #해당 결과물이 있고, 위치변경을 중복으로 하는 것을 방지
    if (len(result) != 0) & (not(stid_ori in temp_id)):
        print(stid_ori, "<-------->", result)
        modifylatlng(result)


sql = "SELECT statid, lat, lng FROM elecgr_info GROUP BY statid"  # LIMIT 0, 100 / WHERE addcd IS NULL
aws_cursor.executemany("UPDATE elecgr_info SET lat = %s, lng = %s WHERE statid = %s", temp)
aws_db.commit()

print("--- 5단계 작업 %s 초 소요됨. 위경도 수정작업 종료 ---" % (time.time() - start_time_step))
print("--- 누적 %s 초 소요됨. ---" % (time.time() - start_time), "다음작업 진행중..")
'''
'''
# 6번째 : 로컬db에서 받아서 AWS로 업데이트(5초)
# 원 AWS 테이블을 드롭X, 원 AWS 테이블 모든값 삭제후 Insert excutemany

sql = "SELECT " \
      "statnm, statid, chgerid, chgertype, addr, addcd, lat, lng, " \
      "usetime, busiid, businm, busicall, stat, statupddt, powertype, output, " \
      "method, zcode, parkingfree, note, limityn, limitdetail, delyn, deldetail " \
      "FROM elecgrlocal"

sq_cursor.execute(sql)
result = sq_cursor.fetchall()

print(len(result))

aws_db = pymysql.connect(
    host=aws_host,
    port=3306,
    user=aws_user,
    passwd=aws_pw,
    db=aws_db,
    charset='utf8'
)

aws_cursor = aws_db.cursor()
aws_cursor.execute("TRUNCATE TABLE elecgr_info")  # 테이블안에 있는 Row내용 모두 삭제

temp = []

sql = "INSERT INTO elecgr_info " \
      "(statnm, statid, chgerid, chgertype, addr, addcd, lat, lng, " \
      "usetime, busiid, businm, busicall, stat, statupddt, powertype, output, " \
      "method, zcode, parkingfree, note, limityn, limitdetail, delyn, deldetail) " \
      "VALUES " \
      "(%s, %s, %s, %s, %s, %s, %s, %s, " \
      "%s, %s, %s, %s, %s, %s, %s, %s, " \
      "%s, %s, %s, %s, %s, %s, %s, %s)"

aws_cursor.executemany(sql, result)
aws_db.commit()

sq_cursor.close()
sq_db.close()

aws_cursor.close()
aws_db.close()

print("총 %s 초 소요됨. AWS RDS에 DB자료 업로드. 마지막 작업완료 ---------- !!!." % (time.time() - start_time))

# bs은 대소문자를 구별하지 않음
'''
'''
def lambda_handler(event, context):

return {
    'statusCode': 200,
    'body': json.dumps('Hello from Lambda!')
}
'''


