import pymysql
import datetime
import uuid
import json



#def lambda_handler(event, context):


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
#str_no = event["queryStringParameters"]["no"]
#snsid = event["queryStringParameters"]["snsid"]
#appid = event["queryStringParameters"]["appid"]
#nickname = event["queryStringParameters"]["nickname"]
#stid = event["queryStringParameters"]["stid"]
#reviewid = event["queryStringParameters"]["reviewid"]
#review = event["queryStringParameters"]["review"]
#level = event["queryStringParameters"]["level"]
#time = event["queryStringParameters"]["time"]

#no = int(str_no)


dist = ""
no = 10
snsid = ""
appid = ""
nickname = "업데이트된닉네임"
stid = "CV000754"
reviewid = ""
review = "충전소 한적하고 좋습니다. 내용이 업데이트 되었습니당."
level = "1"
time = ""

def checkuser(snsid, appid):
    check = [snsid, appid]
    sql = "SELECT COUNT(*) FROM elecgruserdb WHERE snsid = %s AND appid = %s"
    cursor.execute(sql, check)
    result = cursor.fetchall()
    return result[0]


def firstselect(stid, level):
    select = [stid, level]
    sql = "SELECT COUNT(*) FROM elecgrreviewdb WHERE stid = %s AND level = %s"
    cursor.execute(sql, select)
    result_c = cursor.fetchall()

    sql = "SELECT nickname, review, time FROM elecgrreviewdb WHERE stid = %s AND level = %s ORDER BY time DESC LIMIT 1"
    cursor.execute(sql, select)
    result_f = cursor.fetchall()

    result = result_c[0] + result_f[0]
    return result


def initselect(snsid, appid, stid, level):
    select = [stid, level]

    sql = "SELECT snsid, appid, nickname, stid, reviewid, review, level, time, no FROM elecgrreviewdb WHERE stid = %s AND level = %s ORDER BY no DESC LIMIT 10"
    cursor.execute(sql, select)
    result = cursor.fetchall()
    array_temp = []

    for val in result:
        if (val[0] == snsid) & (val[1] == appid):
            my = (1,)
        else:
            my = (0,)

        check_count = [val[3], val[4], "2"]
        sql = "SELECT COUNT(*) FROM elecgrreviewdb WHERE stid = %s AND reviewid = %s AND level = %s"
        cursor.execute(sql, check_count)
        reply_c = cursor.fetchall()
        r_val = val[2:]
        array_temp.append(r_val + my + reply_c[0])

    return array_temp


def addselect(snsid, appid, stid, level, no):
    select = [stid, level, no]

    sql = "SELECT snsid, appid, nickname, stid, reviewid, review, level, time, no FROM elecgrreviewdb WHERE stid = %s AND level = %s AND no < %s ORDER BY no DESC LIMIT 10"
    cursor.execute(sql, select)
    result = cursor.fetchall()

    array_temp = []

    for val in result:
        if (val[0] == snsid) & (val[1] == appid):
            my = (1,)
        else:
            my = (0,)

        check_count = [val[3], val[4], "2"]
        sql = "SELECT COUNT(*) FROM elecgrreviewdb WHERE stid = %s AND reviewid = %s AND level = %s"
        cursor.execute(sql, check_count)
        result_c = cursor.fetchall()
        r_val = val[2:]
        array_temp.append(r_val + my + result_c[0])

    return array_temp


def insert(snsid, appid, nickname, stid, reviewid, review, level, time):
    insertdb = [snsid, appid, nickname, stid, reviewid, review, level, time]
    sql = "INSERT INTO elecgrreviewdb (snsid, appid, nickname, stid, reviewid, review, level, time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(sql, insertdb)
    aws.commit()
    return None


def update(snsid, appid, nickname, stid, reviewid, review, level):
    updatedb = [nickname, review, snsid, appid, stid, reviewid, level]
    sql = "UPDATE elecgrreviewdb SET nickname = %s, review = %s WHERE snsid = %s AND appid = %s AND stid = %s AND reviewid = %s AND level = %s"
    cursor.execute(sql, updatedb)
    aws.commit()
    return None


def delete(snsid, appid, nickname, stid, reviewid, review, level):
    select = [stid, reviewid, "2"]
    sql = "SELECT COUNT(*) FROM elecgrreviewdb WHERE stid = %s AND reviewid = %s AND level = %s"
    cursor.execute(sql, select)
    result_c = cursor.fetchall()

    if result_c[0][0] == 0:
        deletedb = [snsid, appid, stid, reviewid, level]
        sql = "DELETE FROM elecgrreviewdb WHERE snsid = %s AND appid = %s AND stid = %s AND reviewid = %s AND level = %s"
        cursor.execute(sql, deletedb)
        aws.commit()

    elif result_c[0][0] > 0:
        deletedb = [nickname, review, snsid, appid, stid, reviewid, level]
        sql = "UPDATE elecgrreviewdb SET nickname = %s, review = %s WHERE snsid = %s AND appid = %s AND stid = %s AND reviewid = %s AND level = %s"
        cursor.execute(sql, deletedb)
        aws.commit()

    return None


if dist == "firstselect":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        send = firstselect(stid, level)

elif dist == "initselect":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        send = initselect(snsid, appid, stid, level)

elif dist == "addselect":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        send = addselect(snsid, appid, stid, level, no)

elif dist == "insert":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        reviewid = str((uuid.uuid4()))
        time = str(datetime.datetime.now())
        insert(snsid, appid, nickname, stid, reviewid, review, level, time)
        send = initselect(snsid, appid, stid, level)

elif dist == "update":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        update(snsid, appid, nickname, stid, reviewid, review, level)
        send = ["ok"]

elif dist == "delete":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        nickname = "***"
        review = "삭제되었습니다."
        delete(snsid, appid, nickname, stid, reviewid, review, level)
        send = initselect(snsid, appid, stid, level)

cursor.close()
aws.close()


 #   return {
 #       'statusCode': 200,
 #       'body': json.dumps(send)
 #   }