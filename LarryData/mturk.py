import boto3
from botocore.exceptions import ClientError
import xml.etree.ElementTree as ET
import json
import zlib
import base64
import LarryData.s3 as s3
import LarryData.utils

# Local client
_client = None
# Indicate if we are working in production or sandbox
_production = True
# A local instance of the boto3 session to use
_session = boto3.session.Session()


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
    _session = session if session is not None else boto3.session.Session(**LarryData.utils.copy_non_null_keys(locals()))
    s3.set_session(session=_session)
    _client = None


def client():
    """
    Helper function to retrieve an MTurk client. The client will use the current environment in use by the library,
    production or sandbox.
    :return: boto3 MTurk client
    """
    global _client, _production, _session
    if _client is None:
        if _production:
            _client = _session.client(
                service_name='mturk',
                region_name='us-east-1',
                endpoint_url="https://mturk-requester.us-east-1.amazonaws.com"
            )
        else:
            _client = _session.client(
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
    global _client, _production, _session
    if hit_id:
        mturk = _session.client(
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
            mturk = _session.client(
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


def accept_qualification_request(request_id, value=None, mturk_client=None):
    if mturk_client is None:
        mturk_client = client()
    params = LarryData.utils.map_parameters(locals(), {'request_id': 'QualificationRequestId', 'value': 'IntegerValue'})
    return mturk_client.accept_qualification_request(**params)


def approve_assignment(assignment_id, feedback=None, override_rejection=None, mturk_client=None):
    if mturk_client is None:
        mturk_client = client()
    params = LarryData.utils.map_parameters(locals(), {
        'assignment_id': 'AssignmentId',
        'feedback': 'RequesterFeedback',
        'override_rejection': 'OverrideRejection'
    })
    return mturk_client.approve_assignment(**params)


def associate_qualification_with_worker(qualification_type_id, worker_id, value=None, send_notification=None,
                                        mturk_client=None):
    if mturk_client is None:
        mturk_client = client()
    params = LarryData.utils.map_parameters(locals(), {
        'qualification_type_id': 'QualificationTypeId',
        'worker_id': 'WorkerId',
        'value': 'IntegerValue',
        'send_notification': 'SendNotification'
    })
    return mturk_client.associate_qualification_with_worker(**params)


def create_additional_assignments_for_hit(hit_id, additional_assignments, request_token, mturk_client=None):
    if mturk_client is None:
        mturk_client = client()
    params = LarryData.utils.map_parameters(locals(), {
        'hit_id': 'HITId',
        'additional_assignments': 'NumberOfAdditionalAssignments',
        'request_token': 'UniqueRequestToken'
    })
    return mturk_client.create_additional_assignments_for_hit(**params)


def add_assignments(hit_id, additional_assignments, request_token, mturk_client=None):
    if mturk_client is None:
        mturk_client = client()
    return create_additional_assignments_for_hit(hit_id, additional_assignments, request_token, mturk_client)


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
               hit_layout_parameters=None,
               mturk_client=None):
    if mturk_client is None:
        mturk_client = client()
    params = LarryData.utils.map_parameters(locals(), {
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
                    qualification_requirements=None,
                    mturk_client=None):
    if mturk_client is None:
        mturk_client = client()
    params = LarryData.utils.map_parameters(locals(), {
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


def get_assignment(assignment_id, mturk_client=None):
    """
    Retrieves the Assignment and HIT data for a given AssignmentID. The Assignment data is updated to replace the
    Answer XML with a dict object.
    :param assignment_id: The assignment to retrieve
    :param mturk_client: The MTurk client to use for this request instead of the default client
    :return: A tuple of assignment and hit dicts
    """
    if mturk_client is None:
        mturk_client = client()
    response = mturk_client.get_assignment(AssignmentId=assignment_id)
    return resolve_assignment_answer(response['Assignment']), response['HIT']


def get_account_balance(mturk_client=None):
    if mturk_client is None:
        mturk_client = client()
    return mturk_client.get_account_balance()


def get_hit(hit_id, mturk_client=None):
    """
    Retrieve HIT data for the id
    :param hit_id: ID to retrieve
    :param mturk_client: The MTurk client to use for this request instead of the default client
    :return: A dict containing HIT attributes
    """
    # TODO: add error handling when hit not found
    if mturk_client is None:
        mturk_client = client()
    return mturk_client.get_hit(HITId=hit_id)['HIT']


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
        mturk_client = client()
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


def list_hits(mturk_client=None):
    """
    Retrieves all of the HITs in your account with the exception of those that have been deleted (automatically or by
    request).
    :param mturk_client: The MTurk client to use for this request instead of the default client
    :return: A generator containing the HITs
    """
    if mturk_client is None:
        mturk_client = client()
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
        mturk_client = client()
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


def _extract_response_detail(assignments, identifier, exclude_rejected=True):
    responses = []
    response_count = 0
    work_time = 0
    for assignment in assignments:
        responses.append({
            'WorkerId': assignment['WorkerId'],
            'Response': assignment['Answer'].get(identifier),
            'AssignmentId': assignment['AssignmentId'],
            'HITId': assignment['HITId'],
            'AcceptTime': assignment['AcceptTime'],
            'WorkTime': assignment['WorkTime'],
            'Excluded': assignment['AssignmentStatus'] == 'Rejected'
        })
        if exclude_rejected is False or assignment['AssignmentStatus'] != 'Rejected':
            response_count += 1
            work_time += int(assignment['WorkTime'].total_seconds())
    return {
        'Responses': responses,
        'ResponseCount': response_count,
        'WorkTime': work_time,
        'Identifier': identifier
    }


def _score_text_responses(response_detail):
    scores = {}
    for response in response_detail['Responses']:
        value = response['Response']
        score = scores.get(value, 0)
        scores[value] = score + 1
    return scores


def _consolidate_text_response(assignments, identifier, threshold, exclude_rejected=True):
    response_detail = _extract_response_detail(assignments, identifier, exclude_rejected=exclude_rejected)
    response_detail['ScoredResponses'] = _score_text_responses(response_detail)
    answer = None
    for response, score in response_detail['ScoredResponses'].items():
        if score * 100 / response_detail['ResponseCount'] >= threshold:
            answer = response
            break
    if answer is not None:
        for response in response_detail['Responses']:
            response['Accuracy'] = response['Response'] == answer
    return answer, response_detail


def consolidate_crowd_classifier(hit_id, threshold=60, mturk_client=None, exclude_rejected=True):
    """
    Retrieves Worker responses for a HITId and computes a consolidated answer based on a simple plurality of responses.
    For example, if the HIT has 3 Assignments, and Workers respond with responses of A, A, and B, the resulting
    response would be A since 66.7% of Workers agree on a response of A which is higher than the default threshold of
    60%. If the threshold were set at 80% than None would be returned. Similarly, if Workers responded with A, B, and C,
    the result would be None since none of the answers received 60% of responses. By default, Assignments that have
    already been rejected are ignored for purposes of scoring responses.
    :param hit_id: The HITId to retrieve Assignments for
    :param threshold: A 0-100 percentage value (80 = 80%) to use a a threshold in looking for agreement amongst Workers
    :param mturk_client: The MTurk client to use if you don't want to use the default client
    :param exclude_rejected: Boolean value (default=True) indicating that Assignments that have already been
    rejected should be excluded
    :return: A tuple containing the result and an object with detail on the responses for use in measuring Worker
    accuracy
    """
    if mturk_client is None:
        mturk_client = client()
    return _consolidate_text_response(
        list_assignments_for_hit(hit_id, mturk_client=mturk_client),
        'category.label',
        threshold,
        exclude_rejected=exclude_rejected)


def prepare_requester_annotation(payload, s3_resource=s3.resource(), bucket_identifier=None):
    """
    Converts content into a format that can be inserted into the RequesterAnnotation field of a HIT when it is
    created. Because the RequesterAnnotation field is limited to 255 characters this will automatically format it
    as efficiently as possible depending on the size of the content. If the content is short enough the payload will
    be retained as is. If it's too long, it will attempt to compress it using zlib. And if it's still too long it will
    be stored in a temporary file in S3.

    Note that using this may result in creating a '*LarryData*' bucket in your S3 environment which will require
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


def retrieve_requester_annotation(hit=None, hit_id=None, content=None, delete_temp_file=False,
                                  s3_resource=s3.resource(), mturk_client=None):
    """
    Takes a value from the RequesterAnnotation field that was stored by the prepare_requester_annotation function
    and extracts the relevant payload from the text, compressed bytes, or S3.
    :param hit: The HIT object containing the RequesterAnnotation to retrieve
    :param hit_id: The ID of the HIT to retrieve the RequesterAnnotation for
    :param content: The data stored in the RequesterAnnotation field
    :param delete_temp_file: True if you wish to delete the payload S3 object if one was created.
    :param s3_resource: The S3 resource to use when retrieving annotations if necessary
    :param mturk_client: The MTurk client to use if you don't want to use the default client
    :return: The payload that was originally stored by prepare_requester_annotation
    """
    if mturk_client is None:
        mturk_client = client()
    if hit_id is not None:
        hit = get_hit(hit_id, mturk_client=mturk_client)
    if type(hit) is str:
        content = hit
    elif hit is not None:
        content = hit.get('RequesterAnnotation', '')
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
