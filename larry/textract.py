from larry.core import copy_non_null_keys, resolve_client
from larry.s3 import decompose_uri
from larry.types import Box
import boto3
import io

__client = None
# A local instance of the boto3 session to use
__session = boto3.session.Session()


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


@resolve_client(__get_client, 'client')
def detect_text(file=None, image=None, bucket=None, key=None, uri=None, client=None):
    document = {}
    params = {'Document': document}
    if file:
        if isinstance(file, str):
            with open(file, 'rb') as fp:
                document['Bytes'] = fp.read()
        elif isinstance(file, io.RawIOBase) or isinstance(file, io.BufferedIOBase):
            document['Bytes'] = file.read()
        else:
            raise TypeError('Unexpected file of type {}'.format(type(file)))
    if image:
        if isinstance(image, bytes):
            document['Bytes'] = image
        elif hasattr(image, 'save') and callable(getattr(image, 'save', None)):
            objct = io.BytesIO()
            file.save(objct, format='PNG')
            objct.seek(0)
            document['Bytes'] = file.read()
    (bucket, key) = decompose_uri(uri) if uri else (bucket, key)
    if bucket and key:
        document['S3Object'] = {'Bucket': bucket, 'Name': key}
    response = client.detect_document_text(**params)
    return response


def detect_lines(file=None, image=None, bucket=None, key=None, uri=None, size=None, width=None, height=None,
                 client=None):
    (width, height) = size if size else (width, height)
    blocks = detect_text(file=file, image=image, bucket=bucket, key=key, uri=uri, client=client)['Blocks']
    return [_block_to_box(element, width, height) for element in blocks if element['BlockType'] == 'LINE']


def _block_to_box(block, height, width):
    return Box.from_position(block['Geometry']['BoundingBox'], as_ratio=True, height=height, width=width,
                             text=block['Text'], confidence=block['Confidence'])


@resolve_client(__get_client, 'client')
def start_text_detection(bucket=None, key=None, uri=None, client=None):
    (bucket, key) = decompose_uri(uri) if uri else (bucket, key)
    return client.start_document_text_detection(DocumentLocation={
        'S3Object': {
            'Bucket': bucket,
            'Name': key
        }
    })


@resolve_client(__get_client, 'client')
def get_detected_text(job_id, client=None):
    response = client.get_document_text_detection(JobId=job_id)
    return response


def get_detected_lines(job_id, size=None, width=None, height=None, client=None):
    (width, height) = size if size else (width, height)
    response = get_detected_text(job_id=job_id, client=client)
    if 'Blocks' in response:
        return [_block_to_box(element, width, height) for element in response['Blocks'] if element['BlockType'] == 'LINE']
    else:
        return None
