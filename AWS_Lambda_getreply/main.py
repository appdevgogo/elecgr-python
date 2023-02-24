import pymysql
import datetime
import uuid
import json

host = ''
username = ''
password = ''
database = ''

aws = pymysql.connect(
    host=host,
    port=3306,
    user=username,
    passwd=password,
    db=database,
    charset='utf8',
)
cursor = aws.cursor()

#dist = event["queryStringParameters"]["dist"]
#snsid = event["queryStringParameters"]["snsid"]
#appid = event["queryStringParameters"]["appid"]
#nickname = event["queryStringParameters"]["nickname"]
#stid = event["queryStringParameters"]["stid"]
#reviewid = event["queryStringParameters"]["reviewid"]
#replyid = event["queryStringParameters"]["replyid"]
#reply = event["queryStringParameters"]["reply"]
#level = event["queryStringParameters"]["level"]
#time = event["queryStringParameters"]["time"]


dist = ""
no = 7
snsid = ""
appid = ""
nickname = ""
stid = "CV000754"
reviewid = ""
replyid = ""
reply = "댓글 테스트 입니다. 글이 업데이트 되었습니다."
level = "2"
time = ""

def checkuser(snsid, appid):
    check = [snsid, appid]
    sql = "SELECT COUNT(*) FROM elecgruserdb WHERE snsid = %s AND appid = %s"
    cursor.execute(sql, check)
    result = cursor.fetchall()
    return result[0]


def initselect(snsid, appid, stid, reviewid, level):
    select = [stid, reviewid, level]
    sql = "SELECT no, snsid, appid, nickname, stid, replyid, reply, level, time FROM elecgrreviewdb WHERE stid = %s AND reviewid = %s AND level = %s ORDER BY no LIMIT 10"
    cursor.execute(sql, select)
    result = cursor.fetchall()

    array_temp = []

    for val in result:
        if (val[1] == snsid) & (val[2] == appid):
            my = (1,)
        else:
            my = (0,)
        array_temp.append(val + my)

    print(array_temp)

    return result


def addselect(snsid, appid, stid, reviewid, level, no):
    select = [stid, reviewid, level, no]
    sql = "SELECT no, snsid, appid, nickname, stid, replyid, reply, level, time FROM elecgrreviewdb WHERE stid = %s AND reviewid = %s AND level = %s AND no > %s ORDER BY no LIMIT 10"
    cursor.execute(sql, select)
    result = cursor.fetchall()
    array_temp = []

    for val in result:
        if (val[1] == snsid) & (val[2] == appid):
            my = (1,)
        else:
            my = (0,)
        array_temp.append(val + my)

    print(array_temp)

    return result


def insert(snsid, appid, nickname, stid, reviewid, replyid, reply, level, time):
    insertdb = [snsid, appid, nickname, stid, reviewid, replyid, reply, level, time]
    sql = "INSERT INTO elecgrreviewdb (snsid, appid, nickname, stid, reviewid, replyid, reply, level, time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(sql, insertdb)
    aws.commit()
    return None


def update(snsid, appid, nickname, stid, reviewid, replyid, reply, level):
    updatedb = [nickname, reply, snsid, appid, stid, reviewid, replyid, level]
    sql = "UPDATE elecgrreviewdb SET nickname = %s, reply = %s WHERE snsid = %s AND appid = %s AND stid = %s AND reviewid = %s AND replyid = %s AND level = %s"
    cursor.execute(sql, updatedb)
    aws.commit()
    return None


def delete(snsid, appid, stid, reviewid, replyid, level):

    deletedb = [snsid, appid, stid, reviewid, replyid, level]
    sql = "DELETE FROM elecgrreviewdb WHERE snsid = %s AND appid = %s AND stid = %s AND reviewid = %s AND replyid = %s AND level = %s"
    cursor.execute(sql, deletedb)
    aws.commit()

    return None


if dist == "add":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        replyid = str((uuid.uuid4()))
        time = str(datetime.datetime.now())
        insert(snsid, appid, nickname, stid, reviewid, replyid, reply, level, time)
        send = ["ok"]

elif dist == "initselect":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        send = initselect(snsid, appid, stid, reviewid, level)

elif dist == "addselect":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        send = addselect(snsid, appid, stid, reviewid, level, no)

elif dist == "update":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        # time = str(datetime.datetime.now())
        update(snsid, appid, nickname, stid, reviewid, replyid, reply, level)
        send = ["ok"]

elif dist == "delete":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        delete(snsid, appid, stid, reviewid, replyid, level)
        send = ["ok"]

cursor.close()
aws.close()