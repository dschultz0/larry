import boto3
import json


def send_message(destination, message):
    if type(message) == dict:
        message = json.dumps(message)
    sqs = boto3.client('sqs')
    return sqs.send_message(QueueUrl=destination, MessageBody=message)
