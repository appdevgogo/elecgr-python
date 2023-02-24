import sqlite3
import time
import datetime
import pymysql
import configparser

#법정동 코드 계열화 작업후 AWS RDS에 업로드 작업
#Local_upbcode

def init():

    global start_time, now_time, \
        aws_host, aws_user, aws_pw, aws_dbname, \
        aws_db, aws_cursor, dbname, period


    start_time = time.time()
    now_time = datetime.datetime.now()
    print(now_time, "법정동 코드 계열화 작업후 AWS RDS에 업로드 작업 시작")

    config = configparser.ConfigParser()
    config.read('..\config\evspot_config_220808.ini')

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

    dbname = "elecgr_info"
    tbname = "elecgr_bcode"

    aws_cursor = aws_db.cursor()

    del_table(dbname, tbname)
    new_table(tbname)
    up_bcode()



def del_table(dbname, tbname):
    # 해당 이름의 테이블이 있는지 없는지 확인하는 것
    sql = "SELECT count(*) FROM Information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'" % (dbname, tbname)

    aws_cursor.execute(sql)
    result = aws_cursor.fetchall()

    print(result)

    if 1 == result[0][0]:
        sql = "DROP TABLE %s" % (tbname)
        aws_cursor.execute(sql)
        print("%s 테이블 삭제함" % (tbname))

def new_table(tbname):
    sql = "CREATE TABLE %s " \
          "(" \
          "bcode VARCHAR(20) NOT NULL, " \
          "arr VARCHAR(200) NOT NULL, " \
          "level TINYINT NOT NULL, " \
          "arrt VARCHAR(20) NOT NULL," \
          "lat DOUBLE NOT NULL," \
          "lng DOUBLE NOT NULL," \
          "PRIMARY KEY (bcode)" \
          ")" % (tbname)
    aws_cursor.execute(sql)
    aws_db.commit()
    print("%s 테이블 생성함" % (tbname))

def up_bcode():

    conn_sq = sqlite3.connect('..\Local_M_bcodeoganize\localdb.db')
    cursor_sq = conn_sq.cursor()
    sql = "SELECT * FROM bcodeinfo"
    cursor_sq.execute(sql)
    result = cursor_sq.fetchall()

    cursor_sq.close()
    conn_sq.close()

    print(result)

    cursor_aws = aws_db.cursor()

    sql = "INSERT INTO elecgr_bcode (bcode, arr, level, arrt, lat, lng) VALUES (%s, %s, %s, %s, %s, %s)"

    cursor_aws.executemany(sql, result)
    aws_db.commit()

    cursor_aws.close()
    aws_db.close()


init()