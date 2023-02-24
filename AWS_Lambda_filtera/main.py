import json
import pymysql
from ast import literal_eval

#def lambda_handler(event, context):
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
#aidx = event["queryStringParameters"]["aidx"]

dist = ""
snsid = ""
appid = ""
aidx = 4
init_array = [0, 0, 0, 0, 0, 0]

def checkuser(snsid, appid):
    check = [snsid, appid]
    sql = "SELECT COUNT(*) FROM elecgruserdb WHERE snsid = %s AND appid = %s"
    cursor_user.execute(sql, check)
    result = cursor_user.fetchall()
    return result[0]

def select(snsid, appid):
    select = [snsid, appid]
    sql = "SELECT filteron FROM elecgruserdb WHERE snsid = %s AND appid = %s"
    cursor_user.execute(sql, select)
    result = cursor_user.fetchall()
    return result[0]

def update(jsonstring, snsid, appid):
    updatedb = [jsonstring, snsid, appid]
    sql = "UPDATE elecgruserdb SET filteron = %s WHERE snsid = %s AND appid = %s"
    cursor_user.execute(sql, updatedb)
    aws_user.commit()
    return None


if dist == "initload":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        select_result = select(snsid, appid)

        if select_result[0] == "":
            update(init_array, snsid, appid)

        send = select_result[0]

elif dist == "add":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        select_result = select(snsid, appid)

        if select_result[0] == "":
            roaded_array = init_array

        else:
            roaded_array = literal_eval(select_result[0])

        roaded_array[aidx] = 1
        jsonstring = json.dumps(roaded_array)
        update(jsonstring, snsid, appid)
        send = [1]

elif dist == "remove":
    check_result = checkuser(snsid, appid)

    if check_result[0] == 1:
        select_result = select(snsid, appid)
        roaded_array = literal_eval(select_result[0])
        roaded_array[aidx] = 0
        jsonstring = json.dumps(roaded_array)
        update(jsonstring, snsid, appid)
        send = [1]

cursor_user.close()
aws_user.close()

    # return {
    #     'statusCode': 200,
    #     'body': json.dumps(send)
    # }