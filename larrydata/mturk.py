import boto3
from botocore.exceptions import ClientError
import xml.etree.ElementTree as ET
import json
import zlib
import base64
import larrydata.s3 as s3


def client(environment='prod', hit_id=None):
    if hit_id:
        mturk = boto3.client(
            service_name='mturk',
            region_name='us-east-1',
            endpoint_url="https://mturk-requester.us-east-1.amazonaws.com"
        )
        response = None
        try:
            response = mturk.get_hit(HITId=hit_id)
        except ClientError:
            mturk = boto3.client(
                service_name='mturk',
                region_name='us-east-1',
                endpoint_url="https://mturk-requester-sandbox.us-east-1.amazonaws.com"
            )
            response = mturk.get_hit(HITId=hit_id)

        return mturk, response['HIT']
    if environment == 'sandbox':
        endpoint = "https://mturk-requester-sandbox.us-east-1.amazonaws.com"
    else:
        endpoint = "https://mturk-requester.us-east-1.amazonaws.com"
    return boto3.client(
        service_name='mturk',
        region_name='us-east-1',
        endpoint_url=endpoint
    )


def preview_url(client, hit_type_id):
    if client._endpoint.host == 'https://mturk-requester-sandbox.us-east-1.amazonaws.com':
        return "https://workersandbox.mturk.com/mturk/preview?groupId={}".format(hit_type_id)
    else:
        return "https://worker.mturk.com/mturk/preview?groupId={}".format(hit_type_id)


def add_notification(client, hit_type_id, destination, event_types):
    return client.update_notification_settings(
        HITTypeId=hit_type_id,
        Notification={
            'Destination': destination,
            'Transport': 'SQS' if 'sqs' in destination else 'SNS',
            'Version': '2014-08-15',
            'EventTypes': event_types
        },
        Active=True
    )


def parse_answers(assignment):
    result = {
        'WorkerId': assignment['WorkerId'],
        'WorkTime': assignment['SubmitTime'] - assignment['AcceptTime'],
        'Answer': []
    }

    ns = {'mt': 'http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2005-10-01/QuestionFormAnswers.xsd'}
    root = ET.fromstring(assignment['Answer'])

    for a in root.findall('mt:Answer', ns):
        name = a.find('mt:QuestionIdentifier', ns).text
        value = a.find('mt:FreeText', ns).text
        result['Answer'].append({name: value})
    return result


def score_answers(answers):
    results = {}
    for answer in answers:
        responses = answer['Answer']
        for response in responses:
            for key, value in response.items():
                scores = results.get(key, {})
                score = scores.get(value, 0)
                scores[value] = score + 1
                results[key] = scores
    return results


def count_responses(scores):
    count = 0
    for key, value in scores.items():
        count += value
    return count


def consolidate_answers(answers, threshold):
    scores = score_answers(answers)
    print(scores)
    results = {}
    for key, response_scores in scores.items():
        responses = count_responses(response_scores)
        results[key] = None
        for response, response_score in response_scores.items():
            if response_score * 100 / responses > threshold:
                results[key] = response
                break
    return results


def prepare_requester_annotation(content):
    if type(content) == dict:
        content = json.dumps(content, separators=(',', ':'))

    # Use the annotation as is if possible
    if len(content) < 243:
        print('Using annotation as is')
        return '{'+'"payload":'+content+'}'

    # Attempt to compress it
    compressed = str(base64.b85encode(zlib.compress(content.encode())), 'utf-8')
    if len(compressed) < 238:
        print('Compressed RequesterAnnotation from {} to {}'.format(len(content),len(compressed)))
        return '{"payloadBytes":'+compressed+'}'

    # Else post it to s3
    uri = s3.write_object(content, temp_prefix='mturk_requester_annotation/')
    print('Stored RequesterAnnotation to {}'.format(uri))
    return '{"payloadURI":"'+uri+'"}'


def retrieve_requester_annotation(content):
    print(content)
    if len(content) > 0 and content[:1] == '{':
        content = json.loads(content)
        if 'payload' in content:
            return content['payload']
        if 'payloadBytes' in content:
            return json.loads(zlib.decompress(base64.b85decode(content['payloadBytes'].encode())))
        if 'payloadURI' in content:
            return s3.read_dict(uri=content['payloadURI'])
        return content
    else:
        return json.loads(zlib.decompress(base64.b85decode(content.encode())))


def render_question_xml(arguments, template=None, uri=None):
    try:
        from jinja2 import Template
        if uri:
            template = s3.read_text(uri=uri)
        jinja_template = Template(template)
        return '''
            <HTMLQuestion xmlns="http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2011-11-11/HTMLQuestion.xsd">
            <HTMLContent><![CDATA[{}]]></HTMLContent>
            <FrameHeight>0</FrameHeight>
            </HTMLQuestion>'''.format(jinja_template.render(arguments))
    except ImportError as e:
        # We'll simply raise the ImportError to let the developer know this requires Jinja2 to function
        raise e
