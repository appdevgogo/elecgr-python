import pymysql
import time
import datetime

#법정동 기준으로 현재 충전가능 충전소를 업데이트 하는 함수(이것은 21.1.6일 현재 필터적용이 안되므로 취소)
#AWS_Lambda_CW_updatebcodestat

start_time = time.time()
now_time = datetime.datetime.now()
print(now_time, "작업 시작")

host = ''
username = ''
password = ''
database = ''

aws_code = pymysql.connect(
    host=host,
    port=3306,
    user=username,
    passwd=password,
    db=database,
    charset='utf8',

)

cursor_code = aws_code.cursor()


sql = "SELECT bcode, level FROM elecgrcodedb"

cursor_code.execute(sql)
result = cursor_code.fetchall()


#2

host = ''
username = ''
password = ''
database = ''

aws_db = pymysql.connect(
    host=host,
    port=3306,
    user=username,
    passwd=password,
    db=database,
    charset='utf8'
)

cursor_db = aws_db.cursor()
aws_db.commit()

array = []

for i, val in enumerate(result):
    oricode = str(val[0])
    level = int(val[1])

    if level == 3:
        if oricode[5:] == "00000":
            sbcode = oricode[:5]
        elif oricode[8:] == "00":
            sbcode = oricode[:8]
        else:
            sbcode = oricode
    elif level == 2:
        if oricode[4:] == "000000":
            sbcode = oricode[:4]
        elif oricode[5:] == "00000":
            sbcode = oricode[:5]
        else:
            sbcode = oricode[:8]
    elif level == 1:
        sbcode = oricode[:2]

    ssbcode = sbcode + '%'
    sql = "SELECT COUNT(*) FROM elecgrdb WHERE stat = 2 AND addcd LIKE %s"
    cursor_db.execute(sql, ssbcode)
    res = cursor_db.fetchall()
    print(res[0][0], result[i][0], sbcode)
    array.append([res[0][0], result[i][0]])

cursor_db.close()
aws_db.close()

print("데이터 AWS에 업데이트중.. ")

sql = "UPDATE elecgrcodedb SET num = %s WHERE bcode = %s"
cursor_code.executemany(sql, array)
aws_code.commit()

cursor_code.close()
aws_code.close()

print("%s 초 소요됨" % (time.time() - start_time))