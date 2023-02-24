import json
import pymysql
from ast import literal_eval


host = ''
username = ''
password = ''
database = ''

aws_user = pymysql.connect(
    host=host,
    port=3306,
    user=username,
    passwd=password,
    db=database,
    charset='utf8',
)

cursor_user = aws_user.cursor()

#dist = event["queryStringParameters"]["dist"]
#snsid = event["queryStringParameters"]["snsid"]
#appid = event["queryStringParameters"]["appid"]
#stnm_raw = event["queryStringParameters"]["stnm"]
#stid = event["queryStringParameters"]["stid"]

dist = ""
snsid = ""
appid = ""
stnm = "환경부"
stid = "KT"


def checkuser(snsid, appid):
    check = [snsid, appid]
    sql = "SELECT COUNT(*) FROM elecgruserdb WHERE snsid = %s AND appid = %s"
    cursor_user.execute(sql, check)
    result = cursor_user.fetchall()
    return result[0]


def initload():
    sql = "SELECT stid, stnm FROM elecgrfilterdb"
    cursor_user.execute(sql)
    result = cursor_user.fetchall()
    return result


def select(snsid, appid):
    select = [snsid, appid]
    sql = "SELECT filter FROM elecgruserdb WHERE snsid = %s AND appid = %s"
    cursor_user.execute(sql, select)
    result = cursor_user.fetchall()
    return result[0]


def update(jsonstring, snsid, appid):
    updatedb = [jsonstring, snsid, appid]
    sql = "UPDATE elecgruserdb SET filter = %s WHERE snsid = %s AND appid = %s"
    cursor_user.execute(sql, updatedb)
    aws_user.commit()
    # result = check(snsid, appid, stid)
    return None


if dist == "initload":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        select_result = select(snsid, appid)
        print(select_result[0])

        init = initload()
        print(init)

        init_array = []

        for i in init:
            idx = select_result[0].find(i[0])
            if idx == -1:
                print(i[0] + "없다")
                print((i + ("1", )))
                init_array.append(i + ("0", ))
            else:
                print(i[0] + "있다")
                init_array.append(i + ("1",))

        send = init_array

elif dist == "add":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        select_result = select(snsid, appid)
        roaded = list(select_result)

        if select_result[0] == "":
            roaded.append(stid)
            roaded.pop(0)
            roaded_array = roaded

        else:
            roaded_array = literal_eval(roaded[0])
            roaded_array.append(stid)

        jsonstring = json.dumps(roaded_array)
        update(jsonstring, snsid, appid)
        send = select(snsid, appid)

elif dist == "remove":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        select_result = select(snsid, appid)
        roaded = list(select_result)
        roaded_array = literal_eval(roaded[0])
        roaded_array.remove(stid)
        jsonstring = json.dumps(roaded_array)
        update(jsonstring, snsid, appid)
        send = select(snsid, appid)

print(send)

cursor_user.close()
aws_user.close()