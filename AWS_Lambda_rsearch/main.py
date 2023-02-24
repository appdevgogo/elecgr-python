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

#snsid = event["queryStringParameters"]["snsid"]
#appid = event["queryStringParameters"]["appid"]
#dist = event["queryStringParameters"]["dist"]
#group = event["queryStringParameters"]["group"]
#name = event["queryStringParameters"]["name"]
#rsid = event["queryStringParameters"]["rsid"]
#lat = event["queryStringParameters"]["lat"]
#lng = event["queryStringParameters"]["lng"]


snsid = ""
appid = ""
dist = ""
group = "3" #1(네이버 검색),2(법정동검색),3(충전소검색)
name = ""
rsid = ""
lat = "0.0"
lng = "0.0"

def checkall(snsid, appid):
    check = [snsid, appid]
    sql = "SELECT COUNT(*) FROM elecgruserdb WHERE snsid = %s AND appid = %s"
    cursor_user.execute(sql, check)
    result = cursor_user.fetchall()
    return result[0]


def check(snsid, appid, rsid):
    check = [snsid, appid, "%" + rsid + "%"]
    sql = "SELECT COUNT(*) FROM elecgruserdb WHERE snsid = %s AND appid = %s AND rsearch LIKE %s"
    cursor_user.execute(sql, check)
    result = cursor_user.fetchall()
    return result[0]


def select(snsid, appid):
    select = [snsid, appid]
    sql = "SELECT rsearch FROM elecgruserdb WHERE snsid = %s AND appid = %s"
    cursor_user.execute(sql, select)
    result = cursor_user.fetchall()
    return result[0]


def update(jsonstring, snsid, appid):
    updatedb = [jsonstring, snsid, appid]
    sql = "UPDATE elecgruserdb SET rsearch = %s WHERE snsid = %s AND appid = %s"
    cursor_user.execute(sql, updatedb)
    aws_user.commit()
    # result = check(snsid, appid, stid)
    return None


if dist == "check":
    send = check(snsid, appid, rsid)

elif dist == "select":
    select_result = select(snsid, appid)
    roaded = json.loads(select_result[0])
    send = roaded

elif dist == "add":
    check_result = check(snsid, appid, rsid)

    if check_result[0] == 0:
        select_result = select(snsid, appid)

        if select_result[0] == "":
            roaded = [[group, name, rsid, lat, lng]]

        else:
            roaded = json.loads(select_result[0])
            roaded.insert(0, [group, name, rsid, lat, lng])

        jsonstring = json.dumps(roaded)
        update(jsonstring, snsid, appid)

    elif check_result[0] == 1:
        select_result = select(snsid, appid)
        result = select(snsid, appid)
        roaded = json.loads(result[0])
        b = [i[2] for i in roaded]
        c = b.index(rsid)
        roaded.pop(c)
        roaded.insert(0, [group, name, rsid, lat, lng])
        jsonstring = json.dumps(roaded)
        update(jsonstring, snsid, appid)

    send = ["ok"]

elif dist == "remove":
    check_result = check(snsid, appid, rsid)

    if check_result[0] == 1:
        result = select(snsid, appid)
        roaded = json.loads(result[0])
        b = [i[2] for i in roaded]
        c = b.index(rsid)
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


