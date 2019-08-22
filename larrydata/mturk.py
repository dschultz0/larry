import boto3
from botocore.exceptions import ClientError
import xml.etree.ElementTree as ET
import json
import zlib
import base64
import larrydata.s3 as s3


# Local client
_client = None
# Indicate if we are working in production or sandbox
_production = True


def client():
    """
    Helper function to retrieve an MTurk client. The client will use the current environment in use by the library,
    production or sandbox.
    :return: boto3 MTurk client
    """
    global _client, _production
    if _client is None:
        if _production:
            _client = boto3.client(
                service_name='mturk',
                region_name='us-east-1',
                endpoint_url="https://mturk-requester.us-east-1.amazonaws.com"
            )
        else:
            _client = boto3.client(
                service_name='mturk',
                region_name='us-east-1',
                endpoint_url="https://mturk-requester-sandbox.us-east-1.amazonaws.com"
            )
    return _client


def use_production():
    """
    Indicate that the library should use the production MTurk environment.
    :return: None
    """
    global _production, _client
    _production = True
    _client = None


def use_sandbox():
    """
    Indicate that the library should use the sandbox MTurk environment.
    :return: None
    """
    global _production, _client
    _production = False
    _client = None


def environment():
    return 'production' if _production else 'sandbox'


def set_environment(environment='prod', hit_id=None):
    """
    Set the environment that the library should use.
    :param environment: The MTurk environment that the library should use (production or sandbox)
    :param hit_id: If provided, set the environment based on where the hit_id is present
    :return: If a hit_id is provided the HIT response is returned, else None
    """
    global _client, _production
    if hit_id:
        mturk = boto3.client(
            service_name='mturk',
            region_name='us-east-1',
            endpoint_url="https://mturk-requester.us-east-1.amazonaws.com"
        )
        response = None
        try:
            response = mturk.get_hit(HITId=hit_id)
            _production = True
            _client = mturk
        except ClientError:
            mturk = boto3.client(
                service_name='mturk',
                region_name='us-east-1',
                endpoint_url="https://mturk-requester-sandbox.us-east-1.amazonaws.com"
            )
            response = mturk.get_hit(HITId=hit_id)
            _production = False
            _client = mturk
        return response['HIT']
    elif environment == 'sandbox':
        _production = False
        _client = None
    else:
        _production = True
        _client = None


def get_assignment(assignment_id):
    """
    Retrieves the Assignment and HIT data for a given AssignmentID. The Assignment data is updated to replace the
    Answer XML with a dict object.
    :param assignment_id: The assignment to retrieve
    :return: A tuple of assignment and hit dicts
    """
    response = client().get_assignment(AssignmentId=assignment_id)
    return resolve_assignment_answer(response['Assignment']), response['HIT']


def get_hit(hit_id):
    """
    Retrieve HIT data for the id
    :param hit_id: ID to retrieve
    :return: A dict containing HIT attributes
    """
    # TODO: add error handling when hit not found
    return client().get_hit(HITId=hit_id)['HIT']


def list_assignments_for_hit(hit_id, submitted=True, approved=True, rejected=True):
    """
    Retrieves all of the assignments for a HIT with the Answer XML processed into a dict.
    :param hit_id: ID of the HIT to retrieve
    :param submitted: Boolean indicating if assignments in a state of Submitted should be retrieved, default is True
    :param approved: Boolean indicating if assignments in a state of Approved should be retrieved, default is True
    :param rejected: Boolean indicating if assignments in a state of Rejected should be retrieved, default is True
    :return: A generator containing the assignments
    """
    statuses = []
    if submitted:
        statuses.append('Submitted')
    if approved:
        statuses.append('Approved')
    if rejected:
        statuses.append('Rejected')
    pages_to_get = True
    next_token = None
    while pages_to_get:
        if next_token:
            response = client().list_assignments_for_hit(HITId=hit_id, NextToken=next_token, AssignmentStatuses=statuses)
        else:
            response = client().list_assignments_for_hit(HITId=hit_id, AssignmentStatuses=statuses)
        if response.get('NextToken'):
            next_token = response['NextToken']
        else:
            pages_to_get = False
        for assignment in response.get('Assignments',[]):
            yield resolve_assignment_answer(assignment)


def list_hits():
    """
    Retrieves all of the HITs in your account with the exception of those that have been deleted (automatically or by
    request).
    :return: A generator containing the HITs
    """
    pages_to_get = True
    next_token = None
    while pages_to_get:
        if next_token:
            response = client().list_hits(NextToken=next_token)
        else:
            response = client().list_hits()
        if response.get('NextToken'):
            next_token = response['NextToken']
        else:
            pages_to_get = False
        for hit in response.get('HITs',[]):
            yield hit


def preview_url(hit_type_id):
    """
    Generates a preview URL for the hit_type_id using the current environment.
    :param hit_type_id: The HIT type
    :return: The preview URL
    """
    global _production
    if _production:
        return "https://worker.mturk.com/mturk/preview?groupId={}".format(hit_type_id)
    else:
        return "https://workersandbox.mturk.com/mturk/preview?groupId={}".format(hit_type_id)


def add_notification(hit_type_id, destination, event_types):
    """
    Attaches a notification to the HIT type to send a message when various event_types occur
    :param hit_type_id: The HIT type to attach a notification to
    :param destination: An SNS ARN or a SQS URL
    :param event_types: A list of event types to trigger messages on; valid types are:
    AssignmentAccepted | AssignmentAbandoned | AssignmentReturned | AssignmentSubmitted | AssignmentRejected |
    AssignmentApproved | HITCreated | HITExtended | HITDisposed | HITReviewable | HITExpired | Ping
    :return: The API response
    """
    return client().update_notification_settings(
        HITTypeId=hit_type_id,
        Notification={
            'Destination': destination,
            'Transport': 'SQS' if 'sqs' in destination else 'SNS',
            'Version': '2014-08-15',
            'EventTypes': event_types
        },
        Active=True
    )


def resolve_assignment_answer(assignment):
    """
    Parses a QuestionFormAnswers object within an assignment and returns a copy of the assignment dict with the
    answer XML replaced with a dict.
    :param assignment: An MTurk Assignment object
    :return: An MTurk Assignment object with the Answer XML replaced by a dict containing the Worker answer
    """
    result = assignment.copy()
    result['Answer'] = parse_answers(assignment['Answer'])
    if 'AcceptTime' in result and 'SubmitTime' in result:
        result['WorkTime'] = result['SubmitTime'] - result['AcceptTime']
    return result


def parse_answers(answer):
    """
    Parses the answer XML into a usable python dict for analysis. In cases where answers contain JSON strings
    it attempts to parse them into dicts. Does not support file upload answers.
    :param answer: An MTurk Answer object
    :return: A dict containing the parsed answer data
    """
    result = {}
    ns = {'mt': 'http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2005-10-01/QuestionFormAnswers.xsd'}
    root = ET.fromstring(answer)

    # TODO: Need to test this for the various types
    for a in root.findall('mt:Answer', ns):
        name = a.find('mt:QuestionIdentifier', ns).text
        if a.find('mt:FreeText', ns) is not None:
            answer_text = a.find('mt:FreeText', ns).text
            try:
                result[name] = json.loads(answer_text)
            except json.decoder.JSONDecodeError:
                result[name] = answer_text
        else:
            selection_elements = a.findall('mt:SelectionIdentifier')
            selections = []
            for selection in selection_elements:
                selections.append(selection.text)
            if a.find('mt:OtherSelection'):
                selections.append(a.find('mt:OtherSelection').text)
            result[name] = selections
    return result


def _score_answers(answers):
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


def _count_responses(scores):
    count = 0
    for key, value in scores.items():
        count += value
    return count


def _consolidate_answers(answers, threshold):
    scores = _score_answers(answers)
    print(scores)
    results = {}
    for key, response_scores in scores.items():
        responses = _count_responses(response_scores)
        results[key] = None
        for response, response_score in response_scores.items():
            if response_score * 100 / responses > threshold:
                results[key] = response
                break
    return results


def prepare_requester_annotation(payload):
    """
    Converts content into a format that can be inserted into the RequesterAnnotation field of a HIT when it is
    created. Because the RequesterAnnotation field is limited to 255 characters this will automatically format it
    as efficiently as possible depending on the size of the content. If the content is short enough the payload will
    be retained as is. If it's too long, it will attempt to compress it using zlib. And if it's still too long it will
    be stored in a temporary file in S3.

    Note that using this may result in creating a '*larrydata*' bucket in your S3 environment which will require
    create-bucket permissions for your user. When retrieving the annotation you have the option to request that any
    temp files be deleted.
    :param payload: The content to be stored in the RequesterAnnotation field
    :return: A string value that can be placed in the RequesterAnnotation field
    """

    payload_string = json.dumps(payload, separators=(',', ':')) if type(payload) == dict else payload

    # Use the annotation 'as is' if possible
    if len(payload_string) < 243:
        return json.dumps({'payload': payload}, separators=(',', ':'))

    else:
        # Attempt to compress it
        compressed = str(base64.b85encode(zlib.compress(payload_string.encode())), 'utf-8')
        if len(compressed) < 238:
            return json.dumps({'payloadBytes': compressed}, separators=(',', ':'))

        else:
            # Else post it to s3
            uri = s3.write_temp_object(payload, 'mturk_requester_annotation/')
            return json.dumps({'payloadURI': uri}, separators=(',', ':'))


def retrieve_requester_annotation(content, delete_temp_file = False):
    """
    Takes a value from the RequesterAnnotation field that was stored by the prepare_requester_annotation function
    and extracts the relevant payload from the text, compressed bytes, or S3.
    :param content: The data stored in the RequesterAnnotation field
    :param delete_temp_file: True if you wish to delete the payload S3 object if one was created.
    :return: The payload that was originally stored by prepare_requester_annotation
    """
    if len(content) > 0:
        try:
            content = json.loads(content)
            if 'payload' in content:
                return content['payload']
            elif 'payloadBytes' in content:
                return json.loads(zlib.decompress(base64.b85decode(content['payloadBytes'].encode())))
            elif 'payloadURI' in content:
                results = s3.read_dict(uri=content['payloadURI'])
                if delete_temp_file:
                    s3.delete_object(uri=content['payloadURI'])
                return results
            else:
                return content
        except json.decoder.JSONDecodeError:
            return content
    else:
        return content


def render_jinja_template_question(arguments, template=None, template_uri=None):
    """
    Renders an HTML question task using jinja2 to populate a provided template with the arguments provided.
    :param arguments: A dict containing the values to use in rendering the template. Requires Jinja2 be installed.
    :param template: A string value for the template
    :param template_uri: The URI of an S3 object to use as a template
    :return: The HTMLQuestion XML to use for the task
    """
    try:
        from jinja2 import Template
        if template_uri:
            template = s3.read_str(uri=template_uri)
        jinja_template = Template(template)
        return render_html_question(jinja_template.render(arguments))
    except ImportError as e:
        # We'll simply raise the ImportError to let the developer know this requires Jinja2 to function
        raise e


def render_html_question(html, frame_height=0):
    """
    Renders HTML task content within an HTMLQuestion XML object for use as a task.
    :param html: HTML string to render within the template
    :param frame_height: Frame height to use for the Worker viewport, zero by default to use the whole window
    :return: The rendered HTMLQuestion XML string
    """
    return '''<HTMLQuestion xmlns="http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2011-11-11/HTMLQuestion.xsd">
                <HTMLContent><![CDATA[{}]]></HTMLContent><FrameHeight>{}</FrameHeight>
              </HTMLQuestion>'''.format(html, frame_height)


def render_external_question(url, frame_height=0):
    """
    Renders a URL within an ExternalQuestion XML object for use as a task.
    :param url: The URL of the task to display to Workers
    :param frame_height: Frame height to use for the Worker viewport, zero by default to use the whole window
    :return: The rendered ExternalQuestion XML string
    """
    return '''<ExternalQuestion xmlns="http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2006-07-14/ExternalQuestion.xsd">
                <ExternalURL>{}</ExternalURL><FrameHeight>{}</FrameHeight>
              </ExternalQuestion>'''.format(url, frame_height)
