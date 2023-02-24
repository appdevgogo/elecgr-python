import pymysql
import json

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
#stnm = event["queryStringParameters"]["stnm"]
#stid = event["queryStringParameters"]["stid"]

dist = "add"
snsid = "22211992"
appid = "HmWTXg5"
stnm = "경기도청충전소"
stid = "M3333"

def checkall(snsid, appid):
    check = [snsid, appid]
    sql = "SELECT COUNT(*) FROM elecgruserdb WHERE snsid = %s AND appid = %s"
    cursor_user.execute(sql, check)
    result = cursor_user.fetchall()
    return result[0]

def check(snsid, appid, stid):
    check = [snsid, appid, "%" + stid + "%"]
    sql = "SELECT COUNT(*) FROM elecgruserdb WHERE snsid = %s AND appid = %s AND bookmark LIKE %s"
    cursor_user.execute(sql, check)
    result = cursor_user.fetchall()
    return result[0]

def select(snsid, appid):
    select = [snsid, appid]
    sql = "SELECT bookmark FROM elecgruserdb WHERE snsid = %s AND appid = %s"
    cursor_user.execute(sql, select)
    result = cursor_user.fetchall()
    return result[0]

def update(jsonstring, snsid, appid):
    updatedb = [jsonstring, snsid, appid]
    sql = "UPDATE elecgruserdb SET bookmark = %s WHERE snsid = %s AND appid = %s"
    cursor_user.execute(sql, updatedb)
    aws_user.commit()
    #result = check(snsid, appid, stid)
    return None

if dist == "check":
    send = check(snsid, appid, stid)

elif dist == "select":
    send = select(snsid, appid)

elif dist == "add":
    check_result = check(snsid, appid, stid)

    if check_result[0] == 0:
        select_result = select(snsid, appid)

        if select_result[0] == "":
            roaded = [[stnm, stid]]

        else:
            roaded = json.loads(select_result[0])
            roaded.append([stnm, stid])

        jsonstring = json.dumps(roaded)
        update(jsonstring, snsid, appid)

    send = select(snsid, appid)

elif dist == "remove":
    check_result = check(snsid, appid, stid)

    if check_result[0] == 1:
        result = select(snsid, appid)
        roaded = json.loads(result[0])
        b = [i[1] for i in roaded]
        c = b.index(stid)
        roaded.pop(c)
        jsonstring = json.dumps(roaded)
        update(jsonstring, snsid, appid)

    send = select(snsid, appid)

elif dist == "removeall":
    chekcall_result = checkall(snsid, appid)

    if chekcall_result[0] > 0:
        update("", snsid, appid)

    send = select(snsid, appid)

print(send)

cursor_user.close()
aws_user.close()


