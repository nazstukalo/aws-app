import time

import boto3
from boto3.dynamodb.conditions import Key

import random, string

def randomword(length):
   letters = string.ascii_lowercase
   return ''.join(random.choice(letters) for _ in range(length))

boto3.setup_default_session(profile_name='naz')

s3 = boto3.resource('s3')

# fetch the list of existing buckets
buckets = [b.name for b in s3.buckets.all()]

# creating a bucket in AWS S3
bucket_name = randomword(5)
s3.create_bucket(Bucket= bucket_name, CreateBucketConfiguration={
    'LocationConstraint': 'eu-west-1'})

bucket_file = 'text_file.txt'
downloaded_file = 'file.txt'
uploaded_file = 'new_file.txt'
# create the S3 object
some_data = b'Here is some text'
s3_object = s3.Object(bucket_name, bucket_file)
s3_object.put(Body=some_data)

# read data from the S3 object
object_body = s3_object.get()['Body'].read()
print(object_body)

# upload/download object
s3.Bucket(bucket_name).download_file(bucket_file, downloaded_file)
s3.meta.client.upload_file(downloaded_file, bucket_name, uploaded_file)

# delete object
s3.Object(bucket_name, uploaded_file).delete()
s3.Object(bucket_name, bucket_file).delete()

#delete bucket
s3.Bucket(bucket_name).delete()

#dynamoDB
#create table
dynamodb = boto3.resource('dynamodb')
table = dynamodb.create_table(
        TableName='Employees',
        KeySchema=[
            {
                'AttributeName': 'age',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'emp_name',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'age',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'emp_name',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
dynamodb_client = boto3.client('dynamodb', region_name="eu-west-1")
waiter = dynamodb_client.get_waiter('table_exists')
waiter.wait(TableName = 'Employees')

# create item
response = table.put_item(
    Item={
        'age': 20,
        'emp_name': 'somename',
        'info': {
                'some_data':'some data'
            }
    }
)

# get item
response = table.get_item(Key={'age': 20,'emp_name': 'somename'})
print(response['Item'])

#update item
response = table.update_item(
        Key={
            'age': 20,
            'emp_name': 'somename'
        },
        UpdateExpression="set info.some_data=:d",
        ExpressionAttributeValues={
            ':d': 'here is some new data'
        },
        ReturnValues="UPDATED_NEW"
    )



#query
response = table.query(
    KeyConditionExpression=Key('age').eq(20)
)
print(f"response from query: {response['Items']}")

#scan
response = table.scan()
data = response['Items']
print(f"response from scan: {data}")

#delete item
response = table.delete_item(
            Key={
                'age': 20,
                'emp_name': 'somename'
            }
        )

#delete table
table.delete()


#invoke lambda
client = boto3.client('lambda')
response = client.invoke(
    FunctionName='announcments-app-sam-application-getlambda-EiTfbJ0Pinor',
    InvocationType='Event'
)
print(response['ResponseMetadata']['HTTPStatusCode'])

