import boto3
import os
import uuid

AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_DEFAULT_REGION = ""
AWS_BUCKET_NAME = ""

s3 = boto3.client('s3',
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    region_name = AWS_DEFAULT_REGION)

file = os.path.join(os.getcwd(), 'storage', 'test.png')
filename = os.path.basename(file)

foldername = str(uuid.uuid4()) + '/'

#특정 오브젝트 펄더를 만드는 문구
#s3.put_object(Bucket=AWS_BUCKET_NAME, Key=foldername)

#특정 오브젝트 폴더를 생성하고 및 파일을 업로드 하는 문구
s3.upload_file(file, AWS_BUCKET_NAME, foldername + filename)

#특정 폴더의 파일명들을 가져와 주는 문구
prefix = foldername

result = s3.list_objects_v2(Bucket = AWS_BUCKET_NAME, Prefix = prefix)

filenames = []

for item in result['Contents']:
    print(item['Key'])
    filenames.append(item['Key'].replace(prefix, ''))

print(filenames)

#위에 생성한 폴더와 파일을 삭제하는 문구
s3.delete_object(Bucket = AWS_BUCKET_NAME, Key = foldername + filename)



#아래는 버킷을 생성하는 문구
'''
from botocore.exceptions import ClientError
import glob
def create_s3_bucket(bucket_name):
    print("Creating a bucket... " + bucket_name)

    try:
        response = s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'ap-northeast-2' # Seoul  # us-east-1을 제외한 지역은 LocationConstraint 명시해야함.
            }
        )
        return response
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print("Bucket already exists. skipping..")
        else:
            print("Unknown error, exit..")

response = create_s3_bucket(bucket_name="elecgruser.email.attachment")
print("Bucket : " + str(response))
'''