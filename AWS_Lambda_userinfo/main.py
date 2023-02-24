import pymysql
import random
import string

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


#sns = event["queryStringParameters"]["sns"]
#snsid = event["queryStringParameters"]["snsid"]
#email = event["queryStringParameters"]["email"]
#name = event["queryStringParameters"]["name"]

sns = ""
snsid = ""
email = ""
name = ""
nickname = name
#appid = "a4Gp0fA"

checkid = [sns, snsid]

sql = "SELECT sns, snsid FROM elecgruserdb WHERE sns = %s AND snsid = %s"

cursor_user.execute(sql, checkid)
result = cursor_user.fetchall()

print(len(result))

if len(result) == 0:
    print("userdb 새로 추가")
    appid = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(20))
    print(appid)

    insertdb = [sns, snsid, email, name, nickname, appid]
    sql = "INSERT INTO elecgruserdb (sns, snsid, email, name, nickname, appid) VALUES (%s, %s, %s, %s, %s, %s)"

    cursor_user.execute(sql, insertdb)
    aws_user.commit()

else:
    print("userdb 업데이트")
    updatedb = [email, name, sns, snsid]
    sql = "UPDATE elecgruserdb SET email = %s, name = %s WHERE sns = %s AND snsid = %s"

    cursor_user.execute(sql, updatedb)
    aws_user.commit()

sql = "SELECT nickname, appid FROM elecgruserdb WHERE sns = %s AND snsid = %s"

cursor_user.execute(sql, checkid)
result = cursor_user.fetchall()

print(result)

cursor_user.close()
aws_user.close()
