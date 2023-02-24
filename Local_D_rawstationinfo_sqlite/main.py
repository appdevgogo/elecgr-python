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

'''
def lambda_handler(event, context):

return {
    'statusCode': 200,
    'body': json.dumps('Hello from Lambda!')
}
'''

def init():

    global start_time, now_time, url, key, \
        clientid, clientsecret, \
        sq_cursor, sq_db,\
        aws_host, aws_user, aws_pw, aws_dbname, aws_db, aws_cursor, \
        dbname, tbname_update, tbname_check, period

    start_time = time.time()
    now_time = datetime.datetime.now()
    print(now_time, "작업시작", "진행중..")

    '''
    s3 = boto3.client("s3")
    s3.get_object(Bucket="rawstationinfo-sqlite-db", Key="localdb.db")
    s3.download_file("rawstationinfo-sqlite-db", "localdb.db", "/tmp/localdb.db")

    sq_db = sqlite3.connect('/tmp/localdb.db')
    sq_cursor = sq_db.cursor()
    '''

    sq_db = sqlite3.connect('localdb.db')
    sq_cursor = sq_db.cursor()

    config = configparser.ConfigParser()
    config.read('config_210801_001.ini')

    url = config['DATAGOKR']['ENDPOINT']
    key_d = config['DATAGOKR']['KEY']
    key = parse.quote(key_d, safe='')  # s3 config.ini 에 저장된 decoding key값을 encoding 하는 문장

    clientid = config['NAVERGEO']['CLIENTID']
    clientsecret = config['NAVERGEO']['CLIENTSECRET']

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
        charset='utf8'
    )

    step_01()
    step_02()
    step_03()
    step_04()
    step_06()


# ---------->> 1번째 : 공공데이터를 받아서 로컬 sq db파일에 저장하는 작업(1분 정도 소요)
def step_01():

    # 해당 이름의 테이블이 있는지 없는지 확인하는 것
    sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='elecgrlocalraw'"

    sq_cursor.execute(sql)
    result = sq_cursor.fetchall()

    if 1 == result[0][0]:
        sql = "DROP TABLE elecgrlocalraw"
        sq_cursor.execute(sql)
        print("elecgrlocalraw 테이블 삭제함")

    sql = "CREATE TABLE elecgrlocalraw " \
          "(statnm TEXT, statid TEXT, chgerid INTEGER, chgertype INTEGER, addr TEXT, location TEXT, lat REAL, lng REAL, " \
          "usetime TEXT, busiid TEXT, bnm TEXT, businm TEXT, busicall TEXT, stat INTEGER, " \
          "statupddt TEXT, lasttsdt TEXT, lasttedt TEXT, nowtsdt TEXT, output INTEGER, method TEXT, " \
          "zcode INTEGER, parkingfree TEXT, note TEXT, limityn TEXT, limitdetail INTEGER, delyn TEXT, deldetail TEXT, " \
          "PRIMARY KEY (statid, chgerid)" \
          ")"

    sq_cursor.execute(sql)
    print("elecgrlocalraw 테이블 생성함")

    pageno = 1
    inumofrow = 100
    numofrows = 9999

    params = "&pageNo=%d&numOfRows=%d" % (pageno, inumofrow)

    open_url = url + key + params
    res = requests.get(open_url)
    soup = BeautifulSoup(res.content, 'lxml')  # 파싱 속도를 향상 시키기 위해 'lxml'로 실행(별도 lxml설치 필요)
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
        soup = BeautifulSoup(res.content, 'lxml')
        data = soup.find_all('item')

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

        sql = "INSERT INTO elecgrlocalraw (" \
              "statnm, statid, chgerid, chgertype, addr, location, lat, lng, usetime, busiid, " \
              "bnm, businm, busicall, stat, statupddt, lasttsdt, lasttedt, nowtsdt, output, method, " \
              "zcode, parkingfree, note, limityn, limitdetail, delyn, deldetail)" \
              "VALUES (" \
              "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " \
              "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " \
              "?, ?, ?, ?, ?, ?, ?)"

        sq_cursor.executemany(sql, temp)
        sq_db.commit()

    print("--- 총 %s 초 소요됨. 1번째 작업완료! ---" % (time.time() - start_time), "다음작업 진행중..")


# ---------->> 2번째 : 위경도 법정동 코드를 네이버로 추출하여 Local db에 저장 작업(10분정도 소요)
def step_02():

    sql = "SELECT statid, lat, lng FROM elecgrlocalraw"
    sq_cursor.execute(sql)
    result = sq_cursor.fetchall()

    geoURLS = {}
    temp = []

    for item in result:
        statid = item[0]
        geocode = str(item[2]) + "," + str(item[1])
        furl = "" % (
        clientid, clientsecret, geocode)
        geoURLS[furl] = statid

    def load_url(url):
        res = requests.get(url)
        soup = BeautifulSoup(res.content, 'lxml')
        code = soup.find('id').text
        return code

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(load_url, url): url for url in geoURLS.keys()}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                print(data, geoURLS.get(url))
                temp.append([data, geoURLS.get(url)])

            except Exception as e:
                blank = "0"
                print(blank, geoURLS.get(url))
                temp.append([blank, geoURLS.get(url)])

    print("%s 초 소요됨. 법정동코드 네이버 RGEO에서 추출완료. " % (time.time() - start_time), "다음작업 진행중..")

    # 해당 이름의 테이블이 있는지 없는지 확인하는 것
    sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='elecgrlocaltemp'"

    sq_cursor.execute(sql)
    result = sq_cursor.fetchall()

    if 1 == result[0][0]:
        sql = "DROP TABLE elecgrlocaltemp"
        sq_cursor.execute(sql)
        print("elecgrlocaltemp 테이블 삭제함")

    sql = "CREATE TABLE elecgrlocaltemp (addcd TEXT, statid TEXT)"
    sq_cursor.execute(sql)
    print("elecgrlocaltemp 테이블 생성함")

    sql = "INSERT INTO elecgrlocaltemp (addcd, statid) VALUES (?, ?)"

    sq_cursor.executemany(sql, temp)
    sq_db.commit()

    # sq_cursor.execute("UPDATE elecgrlocalraw SET addcd = (SELECT addcd FROM elecgrlocaltemp WHERE elecgrlocaltemp.statid = elecgrlocalraw.statid)")
    # sq_db.commit()

    # sq_cursor.executemany("UPDATE elecgrlocal SET addcd = ? WHERE statid = ?", temp)
    # sq_db.commit()

    # 해당 이름의 테이블이 있는지 없는지 확인하는 것
    sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='elecgrlocal'"

    sq_cursor.execute(sql)
    result = sq_cursor.fetchall()

    if 1 == result[0][0]:
        sql = "DROP TABLE elecgrlocal"
        sq_cursor.execute(sql)
        print("elecgrlocal 테이블 삭제함")

    sq_cursor.execute("CREATE TABLE elecgrlocal AS "
                      "SELECT "
                      "elecgrlocalraw.statnm, elecgrlocalraw.statid, elecgrlocalraw.chgerid, elecgrlocalraw.chgertype, elecgrlocalraw.addr, elecgrlocalraw.location, elecgrlocalraw.lat, elecgrlocalraw.lng, elecgrlocaltemp.addcd, "
                      "elecgrlocalraw.usetime, elecgrlocalraw.busiid, elecgrlocalraw.bnm, elecgrlocalraw.businm, elecgrlocalraw.busicall, elecgrlocalraw.stat, elecgrlocalraw.statupddt, elecgrlocalraw.lasttsdt, elecgrlocalraw.lasttedt, elecgrlocalraw.nowtsdt,"
                      "elecgrlocalraw.output, elecgrlocalraw.method, elecgrlocalraw.zcode, elecgrlocalraw.parkingfree, elecgrlocalraw.note, elecgrlocalraw.limityn, elecgrlocalraw.limitdetail, elecgrlocalraw.delyn, elecgrlocalraw.deldetail "
                      "FROM elecgrlocalraw, elecgrlocaltemp "
                      "WHERE elecgrlocalraw.statid = elecgrlocaltemp.statid")
    sq_db.commit()
    print("elecgrlocal 테이블 생성함")

    print("--- 총 %s 초 소요됨. 2번째 작업완료! ---" % (time.time() - start_time), "다음작업 진행중..")


# ---------->> 3번째 : 법정동 코드 0 또는 NULL값 위경도 수정 작업
def step_03():

    sql = "SELECT statid, addr FROM elecgrlocal WHERE addcd = '0' OR addcd IS NULL"  # LIMIT 0, 100 / WHERE addcd IS NULL
    sq_cursor.execute(sql)
    result = sq_cursor.fetchall()

    temp = []

    for item in result:
        statid = item[0]
        add = item[1]

        furl = "" % (
            clientid, clientsecret, add)

        res = requests.get(furl).json()
        try:
            lat = res['addresses'][0]['y']
            lng = res['addresses'][0]['x']
            temp.append([lat, lng, statid])
        except:
            print("에러", statid, add)

    sq_cursor.executemany("UPDATE elecgrlocal SET lat = ?, lng = ? WHERE statid = ?", temp)
    sq_db.commit()

    print("총 %s초 소요됨. 법정동코드 0 또는 NULL값 위경도 수정. 3번째 작업완료!" % (time.time() - start_time), "다음작업 진행중..")


# ---------->> 4번째 : 법정동코드 0 또는 NULL값 수정된 충전소 위경도 기준으로 법정동 재추출 작업
def step_04():

    sql = "SELECT statid, lat, lng FROM elecgrlocal WHERE addcd = '0' OR addcd IS NULL"  # LIMIT 0, 100 / WHERE addCd IS NULL
    sq_cursor.execute(sql)
    result = sq_cursor.fetchall()

    temp = []

    for item in result:
        statid = item[0]
        geocode = str(item[2]) + "," + str(item[1])
        furl = "" % (
            clientid, clientsecret, geocode)
        res = requests.get(furl)
        soup = BeautifulSoup(res.content, 'lxml')
        code = soup.find('id').text
        temp.append([code, statid])

    sq_cursor.executemany("UPDATE elecgrlocal SET addcd = ? WHERE statid = ?", temp)
    sq_db.commit()

    print("총 %s초 소요됨. 법정동코드 0 또는 NULL값 수정된 충전소 위경도 기준으로 법정동 재추출. 4번째 작업완료!" % (time.time() - start_time), "다음작업 진행중..")


def step_05():
    '''
    # ---------->> 5번째 : 위경도 겹치는 충전소 다시 받아서 위경도 수정하는 작업
    # 너무 시간이 오래 걸리므로 해당 작업은 Mobile Phone 에서 직접구현 필요
    sql = "SELECT statid, lat, lng FROM elecgrlocal GROUP BY statid"  # LIMIT 0, 100 / WHERE addcd IS NULL
    sq_cursor.execute(sql)
    result = sq_cursor.fetchall()

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
        sql = "SELECT lat, lng, statid FROM elecgrlocal " \
              "WHERE NOT statid = ? " \
              "AND lat <= ? AND lat >= ? AND lng <= ? AND lng >= ? GROUP BY statid"
        sq_cursor.execute(sql, (stid_ori, t_lat, b_lat, r_lng, l_lng))
        result = sq_cursor.fetchall()

        #해당 결과물이 있고, 위치변경을 중복으로 하는 것을 방지
        if (len(result) != 0) & (not(stid_ori in temp_id)):
            print(stid_ori, "<-------->", result)
            modifylatlng(result)


    sql = "SELECT statid, lat, lng FROM elecgrlocal GROUP BY statid"  # LIMIT 0, 100 / WHERE addcd IS NULL
    sq_cursor.executemany("UPDATE elecgrlocal SET lat = ?, lng = ? WHERE statid = ?", temp)
    sq_db.commit()

    print("%s초 소요됨. 위경도 수정작업 종료" % (time.time() - start_time), "다음작업 진행중..")
    '''


# ---------->> 6번째 : 로컬db에서 받아서 AWS로 업데이트(5초)
def step_06():

    del_table("elecgr_info", "elecgr_info")
    new_table("elecgr_info")

    # 원 AWS 테이블을 드롭X, 원 AWS 테이블 모든값 삭제후 Insert excutemany
    sql = "SELECT " \
          "statnm, statid, chgerid, chgertype, addr, location, lat, lng, addcd, usetime, " \
          "busiid, bnm, businm, busicall, stat, statupddt, lasttsdt, lasttedt, nowtsdt, output, " \
          "method, zcode, parkingfree, note, limityn, limitdetail, delyn, deldetail " \
          "FROM elecgrlocal"

    sq_cursor.execute(sql)
    result = sq_cursor.fetchall()

    print(len(result))


    sql = "INSERT INTO elecgr_info " \
          "(statnm, statid, chgerid, chgertype, addr, location, lat, lng, addcd, usetime, " \
          "busiid, bnm, businm, busicall, stat, statupddt, lasttsdt, lasttedt, nowtsdt, output, " \
          "method, zcode, parkingfree, note, limityn, limitdetail, delyn, deldetail) " \
          "VALUES " \
          "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s" \
          "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s" \
          "%s, %s, %s, %s, %s, %s, %s, %s)"

    aws_cursor = aws_db.cursor()
    aws_cursor.executemany(sql, result)
    aws_db.commit()

    sq_cursor.close()
    sq_db.close()

    aws_cursor.close()
    aws_db.close()

    print("총 %s 초 소요됨. AWS RDS에 DB자료 업로드. 마지막 작업완료 ---------- !!!." % (time.time() - start_time))


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
          "statnm VARCHAR(150) NOT NULL, " \
          "statid VARCHAR(20) NOT NULL, " \
          "chgerid TINYINT NOT NULL, " \
          "chgertype TINYINT NOT NULL, " \
          "addr VARCHAR(300) NOT NULL, " \
          "location VARCHAR(400) NOT NULL, " \
          "lat DOUBLE NOT NULL, " \
          "lng DOUBLE NOT NULL, " \
          "addcd VARCHAR(20) NOT NULL, " \
          "usetime VARCHAR(150) NOT NULL, " \
          "busiid VARCHAR(10) NOT NULL, " \
          "bnm VARCHAR(100) NOT NULL, " \
          "businm VARCHAR(100) NOT NULL, " \
          "busicall VARCHAR(50) NOT NULL, " \
          "stat TINYINT NOT NULL, " \
          "statupddt DATETIME NOT NULL," \
          "lasttsdt DATETIME NOT NULL," \
          "lasttedt DATETIME NOT NULL," \
          "nowtsdt DATETIME NOT NULL," \
          "output INT NOT NULL," \
          "method VARCHAR(50) NOT NULL, " \
          "zcode TINYINT NOT NULL, " \
          "parkingfree VARCHAR(10) NOT NULL, " \
          "note VARCHAR(400) NOT NULL, " \
          "limityn VARCHAR(10) NOT NULL, " \
          "limitdetail VARCHAR(150) NOT NULL, " \
          "delyn VARCHAR(10) NOT NULL, " \
          "deldetail VARCHAR(150) NOT NULL, " \
          "PRIMARY KEY (statid, chgerid)" \
          ")" % (tbname)

    aws_cursor.execute(sql)
    aws_db.commit()
    print("%s 테이블 생성함" % (tbname))


init()
# bs은 대소문자를 구별하지 않음




