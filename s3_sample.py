# Copyright 2013. Amazon Web Services, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Import the SDK
import boto3
import uuid
import json
import socket
from datetime import datetime
import calendar
import random
import time

# boto3 offers two different styles of API - Resource API (high-level) and
# Client API (low-level). Client API maps directly to the underlying RPC-style
# service operations (put_object, delete_object, etc.). Resource API provides
# an object-oriented abstraction on top (object.delete(), object.put()).
#
# While Resource APIs may help simplify your code and feel more intuitive to
# some, others may prefer the explicitness and control over network calls
# offered by Client APIs. For new AWS customers, we recommend getting started
# with Resource APIs, if available for the service being used. At the time of
# writing they're available for Amazon EC2, Amazon S3, Amazon DynamoDB, Amazon
# SQS, Amazon SNS, AWS IAM, Amazon Glacier, AWS OpsWorks, AWS CloudFormation,
# and Amazon CloudWatch. This sample will show both styles.
#
# First, we'll start with Client API for Amazon S3. Let's instantiate a new
# client object. With no parameters or configuration, boto3 will look for
# access keys in these places:
#
#    1. Environment variables (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)
#    2. Credentials file (~/.aws/credentials or
#         C:\Users\USER_NAME\.aws\credentials)
#    3. AWS IAM role for Amazon EC2 instance
#       (http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html)

# Get Session Credentials
HOST, PORT = "184.73.82.23", 9999

data = {}
data['cmd'] = 'get_firehose_key'
data['uid'] = '40474956106ff5bae7cdbbadf6f4e31a'

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(2)
sock.sendto(bytes(json.dumps(data), "utf-8"), (HOST, PORT))
received = str(sock.recv(1024), "utf-8")

response = json.loads(received, encoding="utf-8")

print(response)

# Create Client 
firehoseClient = boto3.client(
    'firehose',
    aws_session_token = response['session_token'],
    aws_access_key_id = response['access_key'],
    aws_secret_access_key= response['secret_key'],
    region_name='us-east-1')

# Get Firehose Stream Name
firehoseStreamName = response['stream_name']

# Wait for stream to become active
while True:
    streamStatus = firehoseClient.describe_delivery_stream(DeliveryStreamName=firehoseStreamName)
    print (streamStatus)
    if streamStatus['DeliveryStreamDescription']['DeliveryStreamStatus'] == 'ACTIVE':
        break

 
def put_to_stream(thing_id, property_value, property_timestamp):
    payload = {
        'prop': str(property_value),
        'timestamp': str(property_timestamp),
        'thing_id': thing_id}

    print( payload )

    data = json.dumps(payload)
    put_response = firehoseClient.put_record(
        DeliveryStreamName=firehoseStreamName,
        Record={
            'Data' :  data })
    print ( put_response )


for _ in range(10000):
    property_value = random.randint(40, 120)
    property_timestamp = calendar.timegm(datetime.utcnow().timetuple())
    thing_id = 'aa-bb'

    put_to_stream(thing_id, property_value, property_timestamp)

    # wait for 5 second
    time.sleep(0.5)


exit (0)