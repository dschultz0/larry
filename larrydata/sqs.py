import json
import larrydata


_client = None


def client():
    global _client
    if _client is None:
        _client = larrydata.session().client('sqs')
    return _client


def send_message(destination, message):
    if type(message) == dict:
        message = json.dumps(message)
    sqs = client()
    return sqs.send_message(QueueUrl=destination, MessageBody=message)
