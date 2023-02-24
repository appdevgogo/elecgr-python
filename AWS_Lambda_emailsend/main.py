
import smtplib

import base64
import imghdr

import boto3

from email import encoders
from email.utils import formataddr
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

with open('./storage/test.png', 'rb') as img:
    base64_string = base64.b64encode(img.read())

#emailto = event["queryStringParameters"]["emailto"]
#title = event["queryStringParameters"]["title"]
#context = event["queryStringParameters"]["context"]
#dist = event["queryStringParameters"]["dist"]
#first = event["queryStringParameters"]["first"]
#second = event["queryStringParameters"]["second"]
#third = event["queryStringParameters"]["third"]

emailto = 'test@naver.com'
stid = "M43525"
stnm = "강남역 5번출구 충전소"
context = "안녕하세요 먼저 수고가 많으십니다. 어제 12월19일 강남역 5번출구 전기차 충전소에서 충전을 하였는데 충전도중 방전이 발생하였습니다."
dist = "2"
first = base64_string
second = base64_string
third = base64_string


AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_DEFAULT_REGION = ""
AWS_BUCKET_NAME = ""

s3 = boto3.client('s3',
aws_access_key_id = AWS_ACCESS_KEY_ID,
aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
region_name = AWS_DEFAULT_REGION)

file_obj = s3.get_object(Bucket = AWS_BUCKET_NAME, Key = "")
file = file_obj['Body'].read()
#print(file)

print("-------------------------->>")

base64_string = base64.b64encode(file)
#print(base64_string)

#print("-------------------------->>")

oringaltest = base64.b64decode(base64_string)

print(oringaltest)

#temp_array = [base64_string]


temp_array = []

if dist == "1":
    temp_array = [first]

elif dist == "2":
    temp_array = [first, second]

elif dist == "3":
    temp_array = [first, second, third]


from_addr = formataddr(('전기차충전소', 'elecgruser@gmail.com'))
to_addr = emailto

session = None

try:
    # SMTP 세션 생성
    session = smtplib.SMTP('smtp.gmail.com', 587)
    session.set_debuglevel(True)

    # SMTP 계정 인증 설정
    session.ehlo()
    session.starttls()
    session.login('', '')

    # 메일 콘텐츠 설정
    message = MIMEMultipart("mixed")

    # 메일 송/수신 옵션 설정
    message.set_charset('utf-8')
    message['From'] = from_addr
    message['To'] = to_addr
    message['Subject'] = '전기차 충전소 고장신고내역 접수 안내'

    # 메일 콘텐츠 - 내용
    body = '''
    <h2>안녕하세요. 전기차 충전소 운영자 입니다.</h1>
    <h2>요청하신 고장신고 내역이 정상적으로 접수 되었습니다.</h1>
    <h2>신고내역</h1>
    <h2>충전소ID : %s</h1>
    <h2>충전소이름 : %s</h1>
    <h2>내용 : %s</h1>
    ''' % (stid, stnm, context)

    bodyPart = MIMEText(body, 'html', 'utf-8')
    message.attach(bodyPart)

    if len(temp_array) > 0:

        for idx, val in enumerate(temp_array):
            original_img = base64.b64decode(val)
            extension = imghdr.what(None, h=original_img)

            attach_binary = MIMEBase("application", "octect-stream")
            attach_binary.set_payload(original_img)
            encoders.encode_base64(attach_binary)
            attach_binary.add_header("Content-Disposition", 'attachment', filename=('utf-8', '', f'{idx + 1}.' + extension))
            message.attach(attach_binary)

    # 메일 발송
    session.sendmail(from_addr, to_addr, message.as_string())
    print('Successfully sent the mail!!!')

except Exception as e:
    print(e)

finally:
    if session is not None:
        session.quit()

