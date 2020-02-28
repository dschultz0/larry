import boto3
from botocore.exceptions import ClientError
import xml.etree.ElementTree as ET
import json
import zlib
import base64
from enum import Enum
from collections.abc import Iterable
from larry.mturk.HIT import HIT
from larry.mturk.Assignment import Assignment
from larry import utils
from larry import s3
from larry.types import Box
from collections import Mapping
import datetime


# Local client
client = None
# Indicate if we are working in production or sandbox
__production = True
# A local instance of the boto3 session to use
__session = boto3.session.Session()

PRODUCTION = 'production'
SANDBOX = 'sandbox'


def set_session(aws_access_key_id=None, aws_secret_access_key=None, aws__session_token=None,
                region_name=None, profile_name=None, boto_session=None):
    """
    Sets the boto3 session for this module to use a specified configuration state.
    :param aws_access_key_id: AWS access key ID
    :param aws_secret_access_key: AWS secret access key
    :param aws__session_token: AWS temporary session token
    :param region_name: Default region when creating new connections
    :param profile_name: The name of a profile to use
    :param boto_session: An existing session to use
    :return: None
    """
    global __session, client
    __session = boto_session if boto_session else boto3.session.Session(**utils.copy_non_null_keys(locals()))
    s3.set_session(boto_session=__session)
    __create_client()


def __create_client():
    """
    Helper function to build an MTurk client. The client will use the current environment in use by the library,
    production or sandbox.
    """
    global client, __production, __session
    endpoint = "https://mturk-requester.us-east-1.amazonaws.com" if __production \
        else "https://mturk-requester-sandbox.us-east-1.amazonaws.com"
    client = __session.client(
        service_name='mturk',
        region_name='us-east-1',
        endpoint_url=endpoint
    )
    client.production = __production


def use_production():
    """
    Indicate that the library should use the production MTurk environment.
    :return: None
    """
    global __production, client
    __production = True
    __create_client()


def use_sandbox():
    """
    Indicate that the library should use the sandbox MTurk environment.
    :return: None
    """
    global __production, client
    __production = False
    __create_client()


def set_environment(environment=PRODUCTION, hit_id=None):
    """
    Set the environment that the library should use. If a HITId is provided, this will attempt to find
    the HIT in either environment and set the environment where that HITId is present.
    :param environment: The MTurk environment that the library should use (production or sandbox)
    :param hit_id: If provided, set the environment based on where the hit_id is present
    :return: If a hit_id is provided then the HIT response is returned, else None
    """
    def _try_get_hit(_hit_id):
        try:
            return get_hit(_hit_id)
        except ClientError as e:
            return e

    global client, __production, __session
    if hit_id:
        existing_production = __production

        # Try prod first
        if not __production:
            __production = True
            __create_client()
        hit = _try_get_hit(hit_id)
        if isinstance(hit, HIT):
            return hit
        else:
            __production = False
            __create_client()
            sandbox_hit = _try_get_hit(hit_id)
            if isinstance(sandbox_hit, HIT):
                return sandbox_hit
            else:
                __production = existing_production
                raise hit

    elif environment.lower() == SANDBOX:
        __production = False
        __create_client()
    else:
        __production = True
        __create_client()


def mturk_client_environment(c):
    if c is None:
        return None
    elif hasattr(c, 'production'):
        return c.production
    else:
        return c._endpoint.host == 'https://mturk-requester.us-east-1.amazonaws.com'


def production():
    return __production


def sandbox():
    return not __production


def environment():
    return PRODUCTION if __production else SANDBOX


# TODO Review how someone would use this to see if supporting iterables or other object types makes sense
def accept_qualification_request(request_id, value=None, mturk_client=None):
    mturk_client = mturk_client if mturk_client else client
    params = utils.map_parameters(locals(), {'request_id': 'QualificationRequestId', 'value': 'IntegerValue'})
    mturk_client.accept_qualification_request(**params)


def approve(assignment, feedback=None, override_rejection=None, mturk_client=None):
    def _approve_assignment(_assignment_id, _feedback, _override_rejection):
        params = utils.map_parameters(locals(), {
            '_assignment_id': 'AssignmentId',
            '_feedback': 'RequesterFeedback',
            '_override_rejection': 'OverrideRejection'
        })
        mturk_client.approve_assignment(**params)

    mturk_client = mturk_client if mturk_client else client
    if isinstance(assignment, str):
        _approve_assignment(assignment, feedback, override_rejection)
    elif hasattr(assignment, 'AssignmentId'):
        _approve_assignment(assignment['AssignmentId'], feedback, override_rejection)
    else:
        if isinstance(feedback, Iterable):
            for assignment_obj, fbk in zip(assignment, feedback):
                approve(assignment_obj, fbk, override_rejection, mturk_client)
        else:
            for assigment_obj in assignment:
                approve(assigment_obj, feedback, override_rejection, mturk_client)


approve_assignment = approve


def assign_qualification(qualification, worker_id, value=None, send_notification=None, mturk_client=None):
    mturk_client = mturk_client if mturk_client else client
    qualification_type_id = qualification['QualificationTypeId'] if hasattr(qualification,
                                                                            'QualificationTypeId') else qualification

    if isinstance(worker_id, Iterable):
        if isinstance(value, Iterable):
            for worker, val in zip(worker_id, value):
                assign_qualification(qualification_type_id, worker, val, send_notification, mturk_client)
        else:
            for worker in worker_id:
                assign_qualification(qualification_type_id, worker, value, send_notification, mturk_client)
    else:
        params = utils.map_parameters(locals(), {
            'worker_id': 'WorkerId',
            'value': 'IntegerValue',
            'send_notification': 'SendNotification'
        })
        mturk_client.associate_qualification_with_worker(QualificationTypeId=qualification_type_id, **params)


associate_qualification_with_worker = assign_qualification


def add_assignments(hit_id, additional_assignments, request_token=None, mturk_client=None):
    mturk_client = mturk_client if mturk_client else client
    params = utils.map_parameters(locals(), {
        'hit_id': 'HITId',
        'additional_assignments': 'NumberOfAdditionalAssignments',
        'request_token': 'UniqueRequestToken'
    })
    mturk_client.create_additional_assignments_for_hit(**params)


create_additional_assignments_for_hit = add_assignments


def create_hit(title,
               description,
               reward=None,
               reward_cents=None,
               lifetime=86400,
               assignment_duration=3600,
               max_assignments=None,
               auto_approval_delay=None,
               keywords=None,
               question=None,
               annotation=None,
               qualification_requirements=None,
               request_token=None,
               assignment_review_policy=None,
               hit_review_policy=None,
               hit_layout_id=None,
               hit_layout_parameters=None,
               mturk_client=None):
    mturk_client = mturk_client if mturk_client else client
    params = utils.map_parameters(locals(), {
        'title': 'Title',
        'description': 'Description',
        'reward': 'Reward',
        'lifetime': 'LifetimeInSeconds',
        'assignment_duration': 'AssignmentDurationInSeconds',
        'max_assignments': 'MaxAssignments',
        'auto_approval_delay': 'AutoApprovalDelayInSeconds',
        'keywords': 'Keywords',
        'question': 'Question',
        'annotation': 'RequesterAnnotation',
        'qualification_requirements': 'QualificationRequirements',
        'request_token': 'UniqueRequestToken',
        'assignment_review_policy': 'AssignmentReviewPolicy',
        'hit_review_policy': 'HITReviewPolicy',
        'hit_layout_id': 'HITLayoutId',
        'hit_layout_parameters': 'HITLayoutParameters'
    })
    if reward_cents:
        params['reward'] = str(reward_cents / 100)
    return HIT(mturk_client.create_hit(**params).get('HIT'),
               mturk_client=mturk_client,
               production=mturk_client_environment(mturk_client))


def create_hit_type(title,
                    description,
                    reward,
                    lifetime,
                    assignment_duration,
                    max_assignments=None,
                    auto_approval_delay=None,
                    keywords=None,
                    qualification_requirements=None,
                    mturk_client=None):
    mturk_client = mturk_client if mturk_client else client
    params = utils.map_parameters(locals(), {
        'title': 'Title',
        'description': 'Description',
        'reward': 'Reward',
        'lifetime': 'LifetimeInSeconds',
        'assignment_duration': 'AssignmentDurationInSeconds',
        'max_assignments': 'MaxAssignments',
        'auto_approval_delay': 'AutoApprovalDelayInSeconds',
        'keywords': 'Keywords',
        'qualification_requirements': 'QualificationRequirements'
    })
    return mturk_client.create_hit_type(**params).get('HITTypeId')


def _get_assignment(assignment_id, mturk_client=None):
    """
    Retrieves the Assignment and HIT data for a given AssignmentID. The Assignment data is updated to replace the
    Answer XML with a dict object.
    :param assignment_id: The assignment to retrieve
    :param mturk_client: The MTurk client to use for this request instead of the default client
    :return: A tuple of assignment and hit dicts
    """
    mturk_client = mturk_client if mturk_client else client
    response = mturk_client.get_assignment(AssignmentId=assignment_id)
    return response['Assignment'], response['HIT']


def get_assignment(assignment_id, mturk_client=None):
    """
    Retrieves the Assignment and HIT data for a given AssignmentID. The Assignment data is updated to replace the
    Answer XML with a dict object.
    :param assignment_id: The assignment to retrieve
    :param mturk_client: The MTurk client to use for this request instead of the default client
    :return: A tuple of assignment and hit dicts
    """
    a, h = _get_assignment(assignment_id, mturk_client)
    return Assignment(a), HIT(h, mturk_client, mturk_client_environment(mturk_client))


def get_account_balance(mturk_client=None):
    mturk_client = mturk_client if mturk_client else client
    return float(mturk_client.get_account_balance()['AvailableBalance'])


def _get_hit(hit_id, mturk_client=None):
    mturk_client = mturk_client if mturk_client else client
    return mturk_client.get_hit(HITId=hit_id)['HIT'], mturk_client_environment(mturk_client)


def get_hit(hit_id, mturk_client=None):
    """
    Retrieve HIT data for the id
    :param hit_id: ID to retrieve
    :param mturk_client: The MTurk client to use for this request instead of the default client
    :return: A dict containing HIT attributes
    """
    hit, prod = _get_hit(hit_id, mturk_client)
    return HIT(hit, mturk_client=mturk_client, production=prod)


def list_assignments_for_hit(hit_id, submitted=True, approved=True, rejected=True, mturk_client=None):
    """
    Retrieves all of the assignments for a HIT with the Answer XML processed into a dict.
    :param hit_id: ID of the HIT to retrieve
    :param submitted: Boolean indicating if assignments in a state of Submitted should be retrieved, default is True
    :param approved: Boolean indicating if assignments in a state of Approved should be retrieved, default is True
    :param rejected: Boolean indicating if assignments in a state of Rejected should be retrieved, default is True
    :param mturk_client: The MTurk client to use for this request instead of the default client
    :return: A generator containing the assignments
    """
    if mturk_client is None:
        mturk_client = client
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
            response = mturk_client.list_assignments_for_hit(HITId=hit_id, NextToken=next_token,
                                                             AssignmentStatuses=statuses)
        else:
            response = mturk_client.list_assignments_for_hit(HITId=hit_id, AssignmentStatuses=statuses)
        if response.get('NextToken'):
            next_token = response['NextToken']
        else:
            pages_to_get = False
        for assignment in response.get('Assignments', []):
            yield Assignment(assignment)


def list_hits(mturk_client=None):
    """
    Retrieves all of the HITs in your account with the exception of those that have been deleted (automatically or by
    request).
    :param mturk_client: The MTurk client to use for this request instead of the default client
    :return: A generator containing the HITs
    """
    if mturk_client is None:
        mturk_client = client
    pages_to_get = True
    next_token = None
    while pages_to_get:
        if next_token:
            response = mturk_client.list_hits(NextToken=next_token)
        else:
            response = mturk_client.list_hits()
        if response.get('NextToken'):
            next_token = response['NextToken']
        else:
            pages_to_get = False
        for hit in response.get('HITs', []):
            yield HIT(hit, mturk_client, mturk_client_environment(mturk_client))


def create_qualification_type(name, description, keywords=None, status='Active', retry_delay=None, test=None,
                              test_duration=None, answer_key=None, auto_granted=False, auto_granted_value=None,
                              mturk_client=None):
    if mturk_client is None:
        mturk_client = client
    params = utils.map_parameters(locals(), {
        'name': 'Name',
        'keywords': 'Keywords',
        'description': 'Description',
        'status': 'QualificationTypeStatus',
        'retry_delay': 'RetryDelayInSeconds',
        'test': 'Test',
        'answer_key': 'AnswerKey',
        'test_duration': 'TestDurationInSeconds',
        'auto_granted': 'AutoGranted',
        'auto_granted_value': 'AutoGrantedValue'
    })
    response = mturk_client.create_qualification_type(**params)
    return response['QualificationType']['QualificationTypeId']


# TODO: Add methods for multiple applications
def assign_qualification(qualification_type_id, worker_id, value=None, send_notification=False, mturk_client=None):
    if mturk_client is None:
        mturk_client = client
    params = utils.map_parameters(locals(), {
        'qualification_type_id': 'QualificationTypeId',
        'worker_id': 'WorkerId',
        'send_notification': 'SendNotification',
    })
    if value:
        params['IntegerValue'] = value
    return mturk_client.associate_qualification_with_worker(**params)


# TODO: Add methods for multiple applications
def remove_qualification(qualification_type_id, worker_id, reason=None, mturk_client=None):
    if mturk_client is None:
        mturk_client = client
    params = utils.map_parameters(locals(), {
        'qualification_type_id': 'QualificationTypeId',
        'worker_id': 'WorkerId',
        'reason': 'Reason'
    })
    return mturk_client.disassociate_qualification_from_worker(**params)


def preview_url(hit_type_id, production=None):
    """
    Generates a preview URL for the hit_type_id using the current environment.
    :param hit_type_id: The HIT type
    :param production: True if the request is for the production environment
    :return: The preview URL
    """
    global __production
    prod = __production if __production is None else __production
    if prod:
        return "https://worker.mturk.com/mturk/preview?groupId={}".format(hit_type_id)
    else:
        return "https://workersandbox.mturk.com/mturk/preview?groupId={}".format(hit_type_id)


# TODO: Create an enum for destination types
def add_notification(hit_type_id, destination, event_types, mturk_client=None):
    """
    Attaches a notification to the HIT type to send a message when various event_types occur
    :param hit_type_id: The HIT type to attach a notification to
    :param destination: An SNS ARN or a SQS URL
    :param event_types: A list of event types to trigger messages on; valid types are:
    :param mturk_client: The MTurk client to use for this request instead of the default client
    AssignmentAccepted | AssignmentAbandoned | AssignmentReturned | AssignmentSubmitted | AssignmentRejected |
    AssignmentApproved | HITCreated | HITExtended | HITDisposed | HITReviewable | HITExpired | Ping
    :return: The API response
    """
    if mturk_client is None:
        mturk_client = client
    return mturk_client.update_notification_settings(
        HITTypeId=hit_type_id,
        Notification={
            'Destination': destination,
            'Transport': 'SQS' if 'sqs' in destination else 'SNS',
            'Version': '2014-08-15',
            'EventTypes': event_types
        },
        Active=True
    )


def update_expiration(hit_id, expire_at, mturk_client=None):
    mturk_client = mturk_client if mturk_client else client
    mturk_client.update_expiration_for_hit(HITId=hit_id, ExpireAt=expire_at)


def expire_hit(hit_id, mturk_client=None):
    update_expiration(hit_id, datetime.datetime.now(), mturk_client)


def parse_answers(answer):
    """
    Parses the answer XML into a usable python dict for analysis. In cases where answers contain JSON strings
    it attempts to parse them into dicts. Does not support file upload answers.
    :param answer: An MTurk Answer object
    :return: A dict containing the parsed answer data
    """
    def _traverse_dict_for_objs(obj):
        for k, v in obj.items():
            if Box.is_box(v):
                obj[k] = Box.from_obj(v)
            elif isinstance(v, Mapping):
                _traverse_dict_for_objs(v)
            elif isinstance(v, list):
                _traverse_list_for_objs(v)

    def _traverse_list_for_objs(obj):
        for i, v in enumerate(obj):
            if Box.is_box(v):
                obj[i] = Box.from_obj(v)
            elif isinstance(value, Mapping):
                _traverse_dict_for_objs(v)
    result = {}
    ns = {'mt': 'http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2005-10-01/QuestionFormAnswers.xsd'}
    root = ET.fromstring(answer)

    answers = root.findall('mt:Answer', ns)

    # TODO: Need to test this for the various types
    # if this is coming from the crowd-form element load the JSON
    if len(answers) == 1 and answers[0].find('mt:QuestionIdentifier', ns).text == 'taskAnswers':
        answer_body = json.loads(answers[0].find('mt:FreeText', ns).text)

        # For some reason crowd form creates what appears to be an unnecessary list, in those cases we flatten it
        if isinstance(answer_body, list) and len(answer_body) == 1:
            answer_body = answer_body[0]
            for key, value in answer_body.items():
                if isinstance(value, str):
                    try:
                        result[key] = json.loads(value)
                    except json.decoder.JSONDecodeError:
                        result[key] = value
                else:
                    result[key] = value
        else:
            result['taskAnswers'] = answer_body
    # else this is a standard element and each value should be loaded
    else:
        for a in answers:
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
    _traverse_dict_for_objs(result)
    return result


def prepare_requester_annotation(payload, s3_resource=None, bucket_identifier=None):
    """
    Converts content into a format that can be inserted into the RequesterAnnotation field of a HIT when it is
    created. Because the RequesterAnnotation field is limited to 255 characters this will automatically format it
    as efficiently as possible depending on the size of the content. If the content is short enough the payload will
    be retained as is. If it's too long, it will attempt to compress it using zlib. And if it's still too long it will
    be stored in a temporary file in S3.

    Note that using this may result in creating a '*larry*' bucket in your S3 environment which will require
    create-bucket permissions for your user. When retrieving the annotation you have the option to request that any
    temp files be deleted.
    :param payload: The content to be stored in the RequesterAnnotation field
    :param s3_resource: The s3 resource to use for writing to S3 if you don't want to use the default one
    :param bucket_identifier: The identifier to attach to the temp bucket that will be used for writing to s3, typically
    the account id (from STS) for the account being used
    :return: A string value that can be placed in the RequesterAnnotation field
    """
    s3_resource = s3_resource if s3_resource else s3.resource
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
            uri = s3.write_temp_object(payload, 'mturk_requester_annotation/', s3_resource=s3_resource,
                                       bucket_identifier=bucket_identifier)
            return json.dumps({'payloadURI': uri}, separators=(',', ':'))


def retrieve_requester_annotation(hit_id, delete_temp_file=False, s3_resource=None, mturk_client=None):
    """
    Takes a value from the RequesterAnnotation field that was stored by the prepare_requester_annotation function
    and extracts the relevant payload from the text, compressed bytes, or S3.
    :param hit_id: The ID of the HIT to retrieve the RequesterAnnotation for
    :param delete_temp_file: True if you wish to delete the payload S3 object if one was created.
    :param s3_resource: The S3 resource to use when retrieving annotations if necessary
    :param mturk_client: The MTurk client to use if you don't want to use the default client
    :return: The payload that was originally stored by prepare_requester_annotation
    """
    if mturk_client is None:
        mturk_client = client
    hit, p = _get_hit(hit_id, mturk_client=mturk_client)
    return parse_requester_annotation(hit.get('RequesterAnnotation', ''), delete_temp_file, s3_resource)


def parse_requester_annotation(content, delete_temp_file=False, s3_resource=None):
    s3_resource = s3_resource if s3_resource else s3.resource
    if content and len(content) > 0:
        try:
            content = json.loads(content)
            if 'payload' in content:
                return content['payload']
            elif 'payloadBytes' in content:
                return json.loads(zlib.decompress(base64.b85decode(content['payloadBytes'].encode())))
            elif 'payloadURI' in content:
                results = s3.read_dict(uri=content['payloadURI'], s3_resource=s3_resource)
                if delete_temp_file:
                    s3.delete_object(uri=content['payloadURI'], s3_resource=s3_resource)
                return results
            else:
                return content
        except json.decoder.JSONDecodeError:
            return content
    else:
        return content


def render_jinja_template(arguments, template=None, template_uri=None):
    """
    Renders an HTML question task using jinja2 to populate a provided template with the arguments provided.
    :param arguments: A dict containing the values to use in rendering the template. Requires Jinja2 be installed.
    :param template: A string value for the template
    :param template_uri: The URI of an S3 object to use as a template
    :return: The contents of the rendered template
    """
    try:
        from jinja2 import Template
        if template_uri:
            template = s3.read_str(uri=template_uri)
        jinja_template = Template(template)
        jinja_template.environment.policies['json.dumps_kwargs'] = {'cls': utils.JSONEncoder}
        return jinja_template.render(arguments)
    except ImportError as e:
        # We'll simply raise the ImportError to let the developer know this requires Jinja2 to function
        raise e


def render_jinja_template_question(arguments, template=None, template_uri=None):
    """
    Renders an HTML question task using jinja2 to populate a provided template with the arguments provided.
    :param arguments: A dict containing the values to use in rendering the template. Requires Jinja2 be installed.
    :param template: A string value for the template
    :param template_uri: The URI of an S3 object to use as a template
    :return: The HTMLQuestion XML to use for the task
    """
    return render_html_question(render_jinja_template(arguments, template, template_uri))


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


def list_sns_events(event, event_filter=None):
    for record in event['Records']:
        message_id = record['Sns']['MessageId']
        notification = json.loads(record['Sns']['Message'])
        for mturk_event in notification['Events']:
            if event_filter is None or mturk_event['EventType'] == event_filter:
                yield mturk_event, message_id


def list_sqs_events(event, event_filter=None):
    for record in event['Records']:
        message_id = record['MessageId']
        notification = json.loads(record['body'])
        for mturk_event in notification['Events']:
            if event_filter is None or mturk_event['EventType'] == event_filter:
                yield mturk_event, message_id


class QualificationComparitor(Enum):
    LessThan = "LessThan"
    LessThanOrEqualTo = "LessThanOrEqualTo"
    GreaterThan = "GreaterThan"
    GreaterThanOrEqualTo = "GreaterThanOrEqualTo"
    EqualTo = "EqualTo"
    NotEqualTo = "NotEqualTo"
    Exists = "Exists"
    DoesNotExist = "DoesNotExist"
    In = "In"
    NotIn = "NotIn"


class QualificationActionsGuarded(Enum):
    Accept = "Accept"
    PreviewAndAccept = "PreviewAndAccept"
    DiscoverPreviewAndAccept = "DiscoverPreviewAndAccept"


def build_qualification_requirement(qualification_type_id, comparator, values=None, value=None,
                                    locales=None, locale=None, actions_guarded=None):
    if value:
        values = [value]
    if locale:
        locales = [locale]
    requirement = {
        'QualificationTypeId': qualification_type_id,
        'Comparator': comparator.value if isinstance(comparator, QualificationComparitor) else comparator,
    }
    if values:
        requirement['IntegerValues'] = values
    if locales:
        _locales = []
        for locale in locales:
            if isinstance(locale, tuple):
                _locales.append({'Country': locale[0], 'Subdivision': locale[1]})
            else:
                _locales.append({'Country': locale})
        requirement['LocaleValues'] = _locales
    if actions_guarded:
        if isinstance(actions_guarded, QualificationActionsGuarded):
            requirement['ActionsGuarded'] = actions_guarded.value
        else:
            requirement['ActionsGuarded'] = actions_guarded
    return requirement


def build_masters_requirement(actions_guarded=None):
    if __production:
        return build_qualification_requirement('2F1QJWKUDD8XADTFD2Q0G6UTO95ALH',
                                               QualificationComparitor.Exists,
                                               actions_guarded=actions_guarded)
    else:
        return build_qualification_requirement('2ARFPLSP75KLA8M8DH1HTEQVJT3SY6',
                                               QualificationComparitor.Exists,
                                               actions_guarded=actions_guarded)


def build_adult_requirement(actions_guarded=None):
    return build_qualification_requirement('00000000000000000060',
                                           QualificationComparitor.EqualTo,
                                           value=1,
                                           actions_guarded=actions_guarded)


def build_hits_approved_requirement(comparator, value, actions_guarded=None):
    return build_qualification_requirement('00000000000000000040',
                                           comparator,
                                           value=value,
                                           actions_guarded=actions_guarded)


def build_percent_approved_requirement(comparator, value, actions_guarded=None):
    return build_qualification_requirement('000000000000000000L0',
                                           comparator,
                                           value=value,
                                           actions_guarded=actions_guarded)


def build_locale_requirement(comparator, locales=None, locale=None, actions_guarded=None):
    if locale:
        locales = [locale]
    if locales is None or len(locales) == 0:
        raise Exception('A locale qualification requires at least one locale')
    return build_qualification_requirement('00000000000000000071',
                                           comparator=comparator,
                                           locales=locales,
                                           actions_guarded=actions_guarded)


# MTurk utility functions
def retrieve_hit_results_to_dict(items, hit_id_attribute='HITId', assignments_attribute='Assignments'):
    stats = {}
    for item in items:
        if hit_id_attribute in item:
            hit_id = item[hit_id_attribute]
            hit = get_hit(hit_id)
            status = hit['HITStatus']
            item['HITStatus'] = status
            stats[status] = stats.get(status, 0) + 1
            item[assignments_attribute] = list(list_assignments_for_hit(hit_id))
            completed = len(item[assignments_attribute])
            stats['CompletedAssignments'] = stats.get('CompletedAssignments', 0) + completed
            stats['MaxAssignments'] = stats.get('MaxAssignments', 0) + hit['MaxAssignments']
            if completed < hit['MaxAssignments'] and status == 'Reviewable':
                stats['ExpiredAssignments'] = stats.get('ExpiredAssignments', 0) - completed + hit['MaxAssignments']
    return stats
