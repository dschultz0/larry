import boto3
from botocore.exceptions import ClientError
import xml.etree.ElementTree as ET
import json
import zlib
import base64
import larrydata.s3 as s3
import larrydata

# Local client
_client = None
# Indicate if we are working in production or sandbox
_production = True
# A local instance of the boto3 session to use
_session = None


def session():
    """
    Retrieves the current boto3 session for this module
    :return: Boto3 session
    """
    global _session
    if _session is None:
        _session = boto3.session.Session()
    return _session


def set_session(aws_access_key_id=None,
                aws_secret_access_key=None,
                aws_session_token=None,
                region_name=None,
                profile_name=None,
                session=None):
    """
    Sets the boto3 session for this module to use a specified configuration state.
    :param aws_access_key_id: AWS access key ID
    :param aws_secret_access_key: AWS secret access key
    :param aws_session_token: AWS temporary session token
    :param region_name: Default region when creating new connections
    :param profile_name: The name of a profile to use
    :param session: An existing session to use
    :return: None
    """
    global _session, _client
    _session = session if session is not None else boto3.session.Session(**larrydata.copy_non_null_keys(locals()))
    s3.set_session(session=_session)
    _client = None


def client():
    """
    Helper function to retrieve an MTurk client. The client will use the current environment in use by the library,
    production or sandbox.
    :return: boto3 MTurk client
    """
    global _client, _production
    if _client is None:
        if _production:
            _client = session().client(
                service_name='mturk',
                region_name='us-east-1',
                endpoint_url="https://mturk-requester.us-east-1.amazonaws.com"
            )
        else:
            _client = session().client(
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
        mturk = session().client(
            service_name='mturk',
            region_name='us-east-1',
            endpoint_url="https://mturk-requester.us-east-1.amazonaws.com"
        )
        try:
            response = mturk.get_hit(HITId=hit_id)
            _production = True
            _client = mturk
            return response.get('HIT')
        except ClientError:
            mturk = session().client(
                service_name='mturk',
                region_name='us-east-1',
                endpoint_url="https://mturk-requester-sandbox.us-east-1.amazonaws.com"
            )
            response = mturk.get_hit(HITId=hit_id)
            _production = False
            _client = mturk
            return response.get('HIT')

    elif environment.lower() == 'sandbox':
        _production = False
        _client = None
    else:
        _production = True
        _client = None


def _map_parameters(parameters, key_map):
    result = {}
    for k, i in parameters.items():
        if i is not None:
            result[key_map.get(k, k)] = i
    return result


def accept_qualification_request(request_id, value=None, mturk_client=client()):
    params = _map_parameters(locals(), {'request_id': 'QualificationRequestId', 'value': 'IntegerValue'})
    return mturk_client.accept_qualification_request(**params)


def approve_assignment(assignment_id, feedback=None, override_rejection=None, mturk_client=client()):
    params = _map_parameters(locals(), {
        'assignment_id': 'AssignmentId',
        'feedback': 'RequesterFeedback',
        'override_rejection': 'OverrideRejection'
    })
    return mturk_client.approve_assignment(**params)


def associate_qualification_with_worker(qualification_type_id, worker_id, value=None, send_notification=None,
                                        mturk_client=client()):
    params = _map_parameters(locals(), {
        'qualification_type_id': 'QualificationTypeId',
        'worker_id': 'WorkerId',
        'value': 'IntegerValue',
        'send_notification': 'SendNotification'
    })
    return mturk_client.associate_qualification_with_worker(**params)


def create_additional_assignments_for_hit(hit_id, additional_assignments, request_token, mturk_client=client()):
    params = _map_parameters(locals(), {
        'hit_id': 'HITId',
        'additional_assignments': 'NumberOfAdditionalAssignments',
        'request_token': 'UniqueRequestToken'
    })
    return mturk_client.create_additional_assignments_for_hit(**params)


def create_hit(title,
               description,
               reward,
               lifetime,
               assignment_duration,
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
               hit_layout_parameters=None, mturk_client=client()):
    params = _map_parameters(locals(), {
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
    return mturk_client.create_hit(**params).get('HIT')


def create_hit_type(title,
                    description,
                    reward,
                    lifetime,
                    assignment_duration,
                    max_assignments=None,
                    auto_approval_delay=None,
                    keywords=None,
                    qualification_requirements=None, mturk_client=client()):
    params = _map_parameters(locals(), {
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
    return mturk_client.create_hit_type(**params)


def get_assignment(assignment_id, mturk_client=client()):
    """
    Retrieves the Assignment and HIT data for a given AssignmentID. The Assignment data is updated to replace the
    Answer XML with a dict object.
    :param assignment_id: The assignment to retrieve
    :param mturk_client: The MTurk client to use for this request instead of the default client
    :return: A tuple of assignment and hit dicts
    """
    response = mturk_client.get_assignment(AssignmentId=assignment_id)
    return resolve_assignment_answer(response['Assignment']), response['HIT']


def get_account_balance(mturk_client=client()):
    return mturk_client.get_account_balance()


def get_hit(hit_id, mturk_client=client()):
    """
    Retrieve HIT data for the id
    :param hit_id: ID to retrieve
    :param mturk_client: The MTurk client to use for this request instead of the default client
    :return: A dict containing HIT attributes
    """
    # TODO: add error handling when hit not found
    return mturk_client.get_hit(HITId=hit_id)['HIT']


def list_assignments_for_hit(hit_id, submitted=True, approved=True, rejected=True, mturk_client=client()):
    """
    Retrieves all of the assignments for a HIT with the Answer XML processed into a dict.
    :param hit_id: ID of the HIT to retrieve
    :param submitted: Boolean indicating if assignments in a state of Submitted should be retrieved, default is True
    :param approved: Boolean indicating if assignments in a state of Approved should be retrieved, default is True
    :param rejected: Boolean indicating if assignments in a state of Rejected should be retrieved, default is True
    :param mturk_client: The MTurk client to use for this request instead of the default client
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
            response = mturk_client.list_assignments_for_hit(HITId=hit_id, NextToken=next_token,
                                                             AssignmentStatuses=statuses)
        else:
            response = mturk_client.list_assignments_for_hit(HITId=hit_id, AssignmentStatuses=statuses)
        if response.get('NextToken'):
            next_token = response['NextToken']
        else:
            pages_to_get = False
        for assignment in response.get('Assignments', []):
            yield resolve_assignment_answer(assignment)


def list_hits(mturk_client=client()):
    """
    Retrieves all of the HITs in your account with the exception of those that have been deleted (automatically or by
    request).
    :param mturk_client: The MTurk client to use for this request instead of the default client
    :return: A generator containing the HITs
    """
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
            yield hit


def preview_url(hit_type_id, production=None):
    """
    Generates a preview URL for the hit_type_id using the current environment.
    :param hit_type_id: The HIT type
    :param production: True if the request is for the production environment
    :return: The preview URL
    """
    global _production
    prod = _production if production is None else production
    if prod:
        return "https://worker.mturk.com/mturk/preview?groupId={}".format(hit_type_id)
    else:
        return "https://workersandbox.mturk.com/mturk/preview?groupId={}".format(hit_type_id)


def add_notification(hit_type_id, destination, event_types, mturk_client=client()):
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


def prepare_requester_annotation(payload, s3_resource=s3.resource(), bucket_identifier=None):
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
    :param s3_resource: The s3 resource to use for writing to S3 if you don't want to use the default one
    :param bucket_identifier: The identifier to attach to the temp bucket that will be used for writing to s3, typically
    the account id (from STS) for the account being used
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
            uri = s3.write_temp_object(payload, 'mturk_requester_annotation/', s3_resource=s3_resource,
                                       bucket_identifier=bucket_identifier)
            return json.dumps({'payloadURI': uri}, separators=(',', ':'))


def retrieve_requester_annotation(hit=None, content=None, delete_temp_file=False, s3_resource=s3.resource()):
    """
    Takes a value from the RequesterAnnotation field that was stored by the prepare_requester_annotation function
    and extracts the relevant payload from the text, compressed bytes, or S3.
    :param hit: The HIT containing the RequesterAnnotation to retrieve
    :param content: The data stored in the RequesterAnnotation field
    :param delete_temp_file: True if you wish to delete the payload S3 object if one was created.
    :param s3_resource: The S3 resource to use when retrieving annotations if necessary
    :return: The payload that was originally stored by prepare_requester_annotation
    """
    if hit is not None:
        content = hit.get('RequesterAnnotation','')
    if len(content) > 0:
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
