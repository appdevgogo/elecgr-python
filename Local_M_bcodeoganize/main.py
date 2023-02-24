from concurrent.futures import ThreadPoolExecutor
import requests
import sqlite3
import concurrent.futures
import time
import datetime
import configparser

#전국 법정동 코드 전체자료 txt에서 법정동 코드를 실존, 계열화(=level부여) 등 정리를 위히 추출 하고 localdb(sqlite)에 저장
#그후 주소를 기준으로 Naver Geo API를 활용하여 위경도 수치(마커를 생성해야하므로) 추출하여 localdb에 재저장
#Local_M_bcodeoganize (이것은 법정동 새로 갱신 될때 시행, 약 분기1회 또는 반기 1회 정도)

list_f = []

def init():

    global start_time, now_time, period, conn, cursor, id, secret

    start_time = time.time()
    now_time = datetime.datetime.now()
    print(now_time, " 에 법정동코드 계열화 작업시작. ", "진행중.. 약 10분정도 소요예정")

    conn = sqlite3.connect('localdb.db')
    cursor = conn.cursor()

    config = configparser.ConfigParser()
    config.read('..\AWS_Lambda_D_rawstationinfo\config_210801_001.ini')

    id = config['NAVERGEO']['CLIENTID']
    secret = config['NAVERGEO']['CLIENTSECRET']

    road_bcode_file()
    get_latlng()

    cursor.close()
    conn.close()


def road_bcode_file():

    with open("220129_법정동코드 전체자료.txt", "r") as file:
        for line in file:
            (bcode) = line.strip().split("\t")

            if bcode[2] == "존재":
                (ser) = bcode[1].split(" ")

                if len(ser) > 2:
                    level = 3
                else:
                    level = len(ser)

                list_f.append([bcode[0], bcode[1], level, ser[-1]])

    sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='bcodeinfo'"

    cursor.execute(sql)
    result = cursor.fetchall()

    if 1 == result[0][0]:
        sql = "DROP TABLE bcodeinfo"
        cursor.execute(sql)
        print("테이블이 있으므로 테이블 삭제함")

    print("테이블 생성함 또는 테이블이 삭제되어 재생성함")
    sql = "CREATE TABLE bcodeinfo (bcode TEXT, arr TEXT, level INTEGER, arrt TEXT, lat REAL, lng REAL)"
    cursor.execute(sql)

    sql = "INSERT INTO bcodeinfo (bcode, arr, level, arrt) VALUES (?, ?, ?, ?)"

    cursor.executemany(sql, list_f)
    conn.commit()


def get_latlng():

    bcodeURLS = {}
    temp = []

    for item in list_f:
        bcodeid = item[0]
        add = item[1]
        furl = "" % (
            id, secret, add)
        bcodeURLS[furl] = bcodeid

    def load_url(url):
        res = requests.get(url).json()
        lat = res['addresses'][0]['y']
        lng = res['addresses'][0]['x']
        return lat, lng

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(load_url, url): url for url in bcodeURLS.keys()}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                print(data[0], data[1], bcodeURLS.get(url))
                temp.append([data[0], data[1], bcodeURLS.get(url)])

            except Exception as e:
                blank = 0
                print(blank, blank, bcodeURLS.get(url))
                temp.append([blank, blank, bcodeURLS.get(url)])

        print("%s 초 소요됨. 법정동별 위경도 추출완료. " % (time.time() - start_time), "다음작업 진행중..")

    print(temp)
    sql_save = "UPDATE bcodeinfo SET lat = ?, lng = ? WHERE bcode = ?"
    cursor.executemany(sql_save, temp)
    conn.commit()
    print("총 %s 초 소요됨. 위경도 로컬(sqlite3)db에 저장완료. " % (time.time() - start_time))


init()