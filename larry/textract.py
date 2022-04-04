from larry.core import copy_non_null_keys
from larry import s3
from larry.types import Box
import boto3
import io

# A local instance of the boto3 session to use
__session = boto3.session.Session()
__client = __session.client('textract')


def set_session(aws_access_key_id=None,
                aws_secret_access_key=None,
                aws__session_token=None,
                region_name=None,
                profile_name=None,
                boto_session=None):
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
    global __session, __client
    __session = boto_session if boto_session is not None else boto3.session.Session(**copy_non_null_keys(locals()))
    __client = __session.client('textract')


def __getattr__(name):
    if name == 'session':
        return __session
    elif name == 'client':
        return __client
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def __get_client():
    return __client


def detect_text(*location, bucket=None, key=None, uri=None):
    document = {}
    params = {'Document': document}
    if len(location) == 1 and not s3.is_uri(location[0]):
        item = location[0]
        if isinstance(item, str):
            with open(item, 'rb') as fp:
                document['Bytes'] = fp.read()
        elif isinstance(item, io.RawIOBase) or isinstance(item, io.BufferedIOBase):
            document['Bytes'] = item.read()
        elif isinstance(item, bytes):
            document['Bytes'] = item
        elif hasattr(item, 'save') and callable(getattr(item, 'save', None)):
            objct = io.BytesIO()
            item.save(objct, format='PNG')
            objct.seek(0)
            document['Bytes'] = item.read()
        else:
            raise TypeError('Unexpected value of type {}'.format(type(item)))
    else:
        bucket, key, uri = s3.normalize_location(*location, bucket=bucket, key=key, uri=uri)
        if bucket and key:
            document['S3Object'] = {'Bucket': bucket, 'Name': key}
        else:
            raise TypeError("Invalid s3 location")
    response = __client.detect_document_text(**params)
    return response


def detect_lines(*location, bucket=None, key=None, uri=None, size=None, width=None, height=None):
    (width, height) = size if size else (width, height)
    blocks = detect_text(*location, bucket=bucket, key=key, uri=uri)['Blocks']
    return [_block_to_box(element, width, height) for element in blocks if element['BlockType'] == 'LINE']


def _block_to_box(block, width, height, page_indices=None):
    if page_indices and len(page_indices) > 1:
        page = block['Page']
        indices = page_indices[page - 1]
        # Extend from 2 value to 4 value if necessary
        if len(indices) == 2:
            if page == len(page_indices):
                indices.extend([width, height])
            else:
                next_indices = page_indices[page]
                indices.extend([
                    width if indices[0] == next_indices[0] else next_indices[0],
                    height if indices[1] == next_indices[1] else next_indices[1]
                ])
        return Box.from_position_ratio(block['Geometry']['BoundingBox'],
                                       height=indices[3] - indices[1],
                                       width=indices[2] - indices[0],
                                       text=block['Text'],
                                       confidence=block['Confidence']) + [indices[0], indices[1]]
    else:
        return Box.from_position_ratio(block['Geometry']['BoundingBox'],
                                       height=height,
                                       width=width,
                                       text=block['Text'],
                                       confidence=block['Confidence'])


def start_text_detection(*location, bucket=None, key=None, uri=None, sns_topic_arn=None, sns_role_arn=None):
    bucket, key, uri = s3.normalize_location(*location, bucket=bucket, key=key, uri=uri)
    params = {
        "DocumentLocation": {
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        }
    }
    if sns_topic_arn and sns_role_arn:
        params["NotificationChannel"] = {
            'SNSTopicArn': sns_topic_arn,
            'RoleArn': sns_role_arn
        }
    return __client.start_document_text_detection(**params).get('JobId')


def get_detected_text_detail(job_id):
    response = __client.get_document_text_detection(JobId=job_id)
    pages = response.get('DocumentMetadata', {}).get('Pages')
    status = response['JobStatus']
    warnings = response.get('Warnings')
    message = response.get('StatusMessage')
    if status in ['SUCCEEDED', 'PARTIAL_SUCCESS', 'FAILED']:
        result = None if status == 'FAILED' else _block_iterator(job_id, response)
        return True, result, pages, warnings, message
    else:
        return False, None, None, None, None


def get_detected_text(job_id):
    complete, blocks, pages, warnings, message = get_detected_text_detail(job_id)
    return blocks


def _block_iterator(job_id, first_response):
    response = first_response
    blocks_to_retrieve = 'Blocks' in first_response
    while blocks_to_retrieve:
        for block in response['Blocks']:
            yield block
        if 'NextToken' in response:
            response = __client.get_document_text_detection(JobId=job_id, NextToken=response['NextToken'])
        else:
            blocks_to_retrieve = False


def get_detected_lines_detail(job_id, size=None, width=None, height=None, page_indices=None):
    complete, blocks, pages, warnings, message = get_detected_text_detail(job_id)
    if not complete:
        return complete, blocks, pages, warnings, message
    else:
        (width, height) = size if size else (width, height)
        result = None if blocks is None else _line_iterator(blocks, width, height, page_indices)
        return complete, result, pages, warnings, message


def get_detected_lines(job_id, size=None, width=None, height=None, page_indices=None):
    complete, blocks, pages, warnings, message = get_detected_lines_detail(job_id,
                                                                           size,
                                                                           width,
                                                                           height,
                                                                           page_indices)
    return blocks


def _line_iterator(blocks, width=None, height=None, page_indices=None):
    for block in blocks:
        if block['BlockType'] == 'LINE':
            if width and height:
                yield _block_to_box(block, width, height, page_indices).data
            else:
                yield block
