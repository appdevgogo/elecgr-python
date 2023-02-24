import pymysql

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
#nickname = event["queryStringParameters"]["nickname"]

snsid = ""
appid = ""
nickname = ""

select = [snsid, appid, nickname]
sql = "SELECT COUNT(*) FROM elecgruserdb WHERE snsid = %s AND appid = %s AND nickname = %s"
cursor_user.execute(sql, select)
result = cursor_user.fetchall()

if result[0][0] == 0:
    print("사용가능합니다.")
    updatedb = [nickname, snsid, appid]
    sql = "UPDATE elecgruserdb SET nickname = %s WHERE snsid = %s AND appid = %s"
    cursor_user.execute(sql, updatedb)
    aws_user.commit()

    select = [snsid, appid]
    sql = "SELECT nickname FROM elecgruserdb WHERE snsid = %s AND appid = %s"
    cursor_user.execute(sql, select)
    result = cursor_user.fetchall()

elif result[0][0] > 0:
    result = (("1", ),)

print(result[0])

